import time
import json
import oracledb
from utils import load_config, create_producer
from utils.logger_config import setup_logger

logger = setup_logger('producer_sms', 'producer_sms')
config = load_config()
producer = create_producer()
kafka_cfg = config['kafka']
oracle_cfg = config['oracle']

# ====================== ORACLE CONNECTION CHECK ======================
logger.info("🔍 Testing Oracle Database Connection...")

try:
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
    exit(1)

# ====================== Create Connection Pool ======================
logger.info("Creating Oracle Connection Pool...")
pool = oracledb.create_pool(
    user=oracle_cfg['user'],
    password=oracle_cfg['password'],
    dsn=oracle_cfg['dsn'],
    min=10,  # ← Increased from 10
    max=50,  # ← Increased from 50
    increment=2
)

logger.info("🚀 SMS Producer Started - Polling Oracle Database...")

# ====================== Main Polling Loop ======================
messages_produced = 0
poll_count = 0

while True:
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                               SELECT ID, BANK_ID, MESSAGE, MOBILE, RETRY_COUNT
                               FROM RPT.SMS_EMAIL_Q_TABLE
                               WHERE ID IN (SELECT ID
                                            FROM (SELECT ID
                                                  FROM RPT.SMS_EMAIL_Q_TABLE
                                                  WHERE STATUS_SMS = 'P'
                                                    AND (RETRY_COUNT IS NULL OR RETRY_COUNT < :max_retries)
                                                    AND MOBILE IS NOT NULL
                                                  ORDER BY ID ASC)
                                            WHERE ROWNUM <= :batch_size)
                               ORDER BY ID ASC
                               """,
                               max_retries=config['kafka']['max_retries'],
                               batch_size=kafka_cfg.get('batch_size', 10000))

                rows = cursor.fetchmany(kafka_cfg.get('batch_size', 10000))

                if rows:
                    logger.info(f"📊 Poll #{poll_count} - Found {len(rows)} pending messages")
                    poll_count += 1

                    for row in rows:
                        msg_id, bank_id, message, mobile, retry_count = row

                        bank = next((b for b in config['banks']
                                     if str(b['bank_id']).strip() == str(bank_id).strip()), None)
                        if not bank:
                            logger.warning(f"Bank ID {bank_id} not found in config")
                            continue

                        # Update status to 'Q' (Queued)
                        cursor.execute("""
                                       UPDATE RPT.SMS_EMAIL_Q_TABLE
                                       SET STATUS_SMS     = 'Q',
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

                        messages_produced += 1

                    producer.poll(0)
                    producer.flush()

                    if rows:
                        conn.commit()
                        logger.info(f"✅ Committed {len(rows)} messages | Total produced: {messages_produced}")

                        # ✅ WAIT 2 SECONDS AFTER BATCH COMPLETE
                        logger.info(f"⏳ Batch complete. Waiting 2 seconds before next pull...")
                        time.sleep(2)
                else:
                    logger.debug("No pending messages found")
                    time.sleep(1)  # Small wait if no messages

        # No sleep here - immediately try next batch after 2sec wait inside the if block

    except Exception as e:
        logger.error(f"Producer Error: {e}")
        time.sleep(5)