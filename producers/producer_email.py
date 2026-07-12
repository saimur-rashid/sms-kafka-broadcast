import time
import json
import oracledb
from utils import load_config, create_producer
from utils.logger_config import setup_logger

logger = setup_logger('producer_email', 'producer_email')

config = load_config()
producer = create_producer()

kafka_cfg = config['kafka']
oracle_cfg = config['oracle']

# =====================================================
# Test Oracle Connection
# =====================================================

logger.info("🔍 Testing Oracle Database Connection...")

try:
    with oracledb.connect(
        user=oracle_cfg['user'],
        password=oracle_cfg['password'],
        dsn=oracle_cfg['dsn']
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSDATE FROM DUAL")
            server_time = cursor.fetchone()[0]
        logger.info(f"✅ Oracle Connection Successful! Server Time: {server_time}")

except Exception as e:
    logger.error(f"❌ Oracle Connection FAILED: {e}")
    raise SystemExit(1)

# =====================================================
# Connection Pool
# =====================================================

logger.info("Creating Oracle Connection Pool...")

pool = oracledb.create_pool(
    user=oracle_cfg['user'],
    password=oracle_cfg['password'],
    dsn=oracle_cfg['dsn'],
    min=10,
    max=50,
    increment=2
)

def safe_json_serialize(obj):
    """Handle LOB objects and other non-serializable types"""
    if hasattr(obj, 'read'):  # LOB object
        try:
            data = obj.read()
            if isinstance(data, bytes):
                return data.decode('utf-8', errors='replace')
            return data
        except:
            return str(data)
    return str(obj)  # fallback

logger.info("🚀 Email Producer Started")

messages_produced = 0
poll_count = 1

while True:
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cursor:

                # FIX 1: Removed RPT. prefix  (connected as RPT already)
                # FIX 2: STATUS_EMAIL → STATUS (real column name from table schema)
                cursor.execute(
                    """
                    SELECT
                        ID,
                        TXN_DATE,
                        BANK_ID,
                        CARD_NO,
                        EMAIL,
                        SUBJECT,
                        CURRENCY,
                        AMOUNT,
                        MESSAGE,
                        RETRY_COUNT
                    FROM EMAIL_Q_TABLE
                    WHERE ID IN (
                        SELECT ID FROM (
                            SELECT ID
                            FROM EMAIL_Q_TABLE
                            WHERE STATUS = 'P'
                              AND EMAIL IS NOT NULL
                              AND (RETRY_COUNT IS NULL
                                   OR RETRY_COUNT < :max_retries)
                            ORDER BY ID
                        )
                        WHERE ROWNUM <= :batch_size
                    )
                    ORDER BY ID
                    """,
                    max_retries=kafka_cfg['max_retries'],
                    batch_size=kafka_cfg.get('email_batch_size', 10000)
                )

                rows = cursor.fetchall()

                if not rows:
                    logger.debug("No pending email messages found.")
                    time.sleep(1)
                    continue

                logger.info(
                    f"📦 Poll #{poll_count} : Found {len(rows)} pending email messages"
                )
                poll_count += 1

                for row in rows:
                    msg_id, txn_date, bank_id, card_no, email, subject, currency, amount, message, retry_count = row

                    bank = next(
                        (b for b in config['banks']
                         if str(b['bank_id']).strip() == str(bank_id).strip()),
                        None
                    )

                    if bank is None:
                        logger.warning(f"Bank ID {bank_id} not found in config")
                        continue

                    # FIX 2: STATUS_EMAIL → STATUS
                    cursor.execute(
                        """
                        UPDATE EMAIL_Q_TABLE
                        SET    STATUS     = 'Q',
                               UPDATED_AT = SYSTIMESTAMP
                        WHERE  ID = :id
                        """,
                        id=msg_id
                    )

                    payload = {
                        "id":         str(msg_id),
                        "msg_type":   str(subject) if subject else "EMAIL",
                        "txn_date":   txn_date.strftime("%Y-%m-%d") if txn_date else None,
                        "bank_id":    str(bank_id),
                        "card_no":    str(card_no),
                        "email":      email,
                        "message":    message,
                        "subject":    bank.get("email_subject"),
                        "currency":   str(currency) if currency else "BDT",
                        "amount":     amount,
                        "from_email": bank.get("email_from"),
                        "retry_count": int(retry_count or 0),
                        "timestamp":  time.time()
                    }

                    producer.produce(
                        topic=kafka_cfg['email_topic'],
                        key=str(bank_id).encode(),
                        value=json.dumps(payload, default=safe_json_serialize, ensure_ascii=False).encode(),
                        #value=json.dumps(payload).encode(),
                        callback=lambda err, msg:
                            logger.error(f"Delivery Failed: {err}") if err else None
                    )

                    messages_produced += 1

                producer.flush()
                conn.commit()

                logger.info(
                    f"✅ Batch Complete | Sent={len(rows)} | Total Produced={messages_produced}"
                )
                logger.info("⏳ Waiting 2 seconds before next poll...")
                time.sleep(2)

    except Exception as e:
        logger.exception(f"Email Producer Error: {e}")
        time.sleep(5)