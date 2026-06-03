import time
import json
import oracledb
from utils import load_config, create_producer
from utils.logger_config import setup_logger

logger = setup_logger('producer', 'producer')
config = load_config()
producer = create_producer()
kafka_cfg = config['kafka']
oracle_cfg = config['oracle']

# ====================== ORACLE CONNECTION CHECK ======================
logger.info("🔍 Testing Oracle Database Connection...")

try:
    # Test connection
    with oracledb.connect(
            user=oracle_cfg['user'],
            password=oracle_cfg['password'],
            dsn=oracle_cfg['dsn']
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSDATE FROM DUAL")
            result = cursor.fetchone()
            logger.info(f"✅ Oracle Connection Successful! Server Time: {result[0]}")

except Exception as e:
    logger.error(f"❌ Oracle Connection FAILED: {e}")
    logger.error("Please check Oracle credentials, DSN, or network connection.")
    exit(1)  # Stop the program if connection fails

# ====================== Create Connection Pool ======================
logger.info("Creating Oracle Connection Pool...")
pool = oracledb.create_pool(
    user=oracle_cfg['user'],
    password=oracle_cfg['password'],
    dsn=oracle_cfg['dsn'],
    min=2,
    max=10,
    increment=1
)

logger.info("🚀 SMS Producer Started - Polling Oracle Database...")

# ====================== Main Polling Loop ======================
while True:
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                               SELECT ID, BANK_ID, MESSAGE, MOBILE, RETRY_COUNT
                               FROM SMS_Q_TABLE
                               WHERE STATUS = 'P'
                                 AND (RETRY_COUNT IS NULL OR RETRY_COUNT < :max_retries)
                                   FOR UPDATE SKIP LOCKED
                               """, max_retries=config['kafka']['max_retries'])

                rows = cursor.fetchmany(kafka_cfg.get('batch_size', 50))

                logger.info(f"📊 Found {len(rows)} pending messages")

                for row in rows:
                    msg_id, bank_id, message, mobile, retry_count = row

                    bank = next((b for b in config['banks']
                                 if str(b['bank_id']).strip() == str(bank_id).strip()), None)
                    if not bank:
                        logger.warning(f"Bank ID {bank_id} not found in config")
                        continue

                    # Update status to 'Q' (Queued)
                    cursor.execute("""
                                   UPDATE SMS_Q_TABLE
                                   SET STATUS     = 'Q',
                                       UPDATED_AT = SYSDATE
                                   WHERE ID = :msg_id
                                   """, msg_id=str(msg_id))

                    payload = {
                        "id": str(msg_id),
                        "bank_id": str(bank_id).strip(),
                        "phone": mobile,
                        "message": message,
                        "source_id": bank['ulka']['source_id'],
                        "sender": bank['ulka']['sender'],
                        "retry_count": int(retry_count or 0),
                        "timestamp": time.time()
                    }

                    producer.produce(
                        topic=kafka_cfg['sms_topic'],
                        value=json.dumps(payload).encode('utf-8'),
                        key=str(bank_id).encode('utf-8'),
                        callback=lambda err, msg: logger.error(f"Delivery failed: {err}") if err else None
                    )

                    logger.info(f"✅ Produced message ID: {msg_id} | Bank: {bank_id} | phone: {mobile} | message: {message}")

                producer.poll(0)
                producer.flush()
                if rows:
                    conn.commit()
                    logger.info(f"✅ Committed {len(rows)} messages")

        time.sleep(oracle_cfg.get('poll_interval_sec', 2))

    except Exception as e:
        logger.error(f"Producer Error: {e}")
        time.sleep(5)