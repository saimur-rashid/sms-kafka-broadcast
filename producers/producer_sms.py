import time
import json
import oracledb
from utils import load_config, create_producer
from utils.logger_config import setup_logger

logger = setup_logger("producer_sms", "producer_sms")

config = load_config()
producer = create_producer()

kafka_cfg = config["kafka"]
oracle_cfg = config["oracle"]

# =====================================================
# Test Oracle Connection
# =====================================================

logger.info("🔍 Testing Oracle Database Connection...")

try:
    with oracledb.connect(
            user=oracle_cfg["user"],
            password=oracle_cfg["password"],
            dsn=oracle_cfg["dsn"]
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT SYSDATE FROM DUAL")
            server_time = cursor.fetchone()[0]
        logger.info(f"✅ Oracle Connected. Server Time : {server_time}")

except Exception as e:
    logger.error(f"❌ Oracle Connection Failed : {e}")
    raise SystemExit(1)

# =====================================================
# Connection Pool
# =====================================================

logger.info("Creating Oracle Connection Pool...")

pool = oracledb.create_pool(
    user=oracle_cfg["user"],
    password=oracle_cfg["password"],
    dsn=oracle_cfg["dsn"],
    min=10,
    max=50,
    increment=2
)

logger.info("🚀 SMS Producer Started")

messages_produced = 0
poll_count = 1

while True:
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cursor:

                # FIX 1: Removed RPT. prefix  (connected as RPT already)
                # FIX 2: STATUS_SMS → STATUS  (real column name from table schema)
                cursor.execute(
                    """
                    SELECT
                        ID,
                        TXN_DATE,
                        BANK_ID,
                        MOBILE,
                        MESSAGE,
                        RETRY_COUNT
                    FROM SMS_Q_TABLE
                    WHERE ID IN (
                        SELECT ID FROM (
                            SELECT ID
                            FROM SMS_Q_TABLE
                            WHERE STATUS = 'P'
                              AND MOBILE IS NOT NULL
                              AND (RETRY_COUNT IS NULL
                                   OR RETRY_COUNT < :max_retries)
                            ORDER BY ID
                        )
                        WHERE ROWNUM <= :batch_size
                    )
                    ORDER BY ID
                    """,
                    max_retries=kafka_cfg["max_retries"],
                    batch_size=kafka_cfg.get("batch_size", 10000)
                )

                rows = cursor.fetchall()

                if not rows:
                    logger.debug("No pending SMS found.")
                    time.sleep(1)
                    continue

                logger.info(f"📦 Poll #{poll_count} : {len(rows)} SMS found.")
                poll_count += 1

                for row in rows:
                    msg_id, txn_date, bank_id, mobile, message, retry_count = row

                    bank = next(
                        (b for b in config["banks"]
                         if str(b["bank_id"]).strip() == str(bank_id).strip()),
                        None
                    )

                    if bank is None:
                        logger.warning(f"Bank ID {bank_id} not found in config.")
                        continue

                    # FIX 2: STATUS_SMS → STATUS
                    cursor.execute(
                        """
                        UPDATE SMS_Q_TABLE
                        SET    STATUS     = 'Q',
                               UPDATED_AT = SYSTIMESTAMP
                        WHERE  ID = :id
                        """,
                        id=msg_id
                    )

                    payload = {
                        "id":         str(msg_id),
                        "msg_type":   "SMS",
                        "txn_date":   txn_date.strftime("%Y-%m-%d") if txn_date else None,
                        "bank_id":    str(bank_id),
                        "phone":      mobile,
                        "message":    message,
                        "source_id":  bank["ulka"]["source_id"],
                        "sender":     bank["ulka"]["sender"],
                        "retry_count": int(retry_count or 0),
                        "timestamp":  time.time()
                    }

                    producer.produce(
                        topic=kafka_cfg["sms_topic"],
                        key=str(bank_id).encode(),
                        value=json.dumps(payload).encode(),
                        callback=lambda err, msg:
                            logger.error(f"Delivery Failed : {err}") if err else None
                    )

                    messages_produced += 1

                producer.flush()
                conn.commit()

                logger.info(
                    f"✅ Batch Complete | Sent={len(rows)} | Total={messages_produced}"
                )
                logger.info("⏳ Waiting 2 seconds...")
                time.sleep(2)

    except Exception as e:
        logger.exception(f"Producer Error : {e}")
        time.sleep(5)