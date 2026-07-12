import time
import json
import os
import oracledb
from utils.logger_config import setup_logger
from utils import load_config

logger = setup_logger('payment_data_executor', 'payment_data_executor')
config = load_config()
oracle_cfg = config['oracle']

# ====================== STATE FILE SETUP ======================
STATE_FILE = '/app/payment/last_txn_state.json'
APP_STATE_FILE = 'last_txn_state.json'

def get_state_file_path():
    if os.path.exists('/app/payment'):
        return STATE_FILE
    return APP_STATE_FILE


def load_last_txn_id():
    state_file = get_state_file_path()
    try:
        if os.path.exists(state_file):
            with open(state_file, 'r') as f:
                data = json.load(f)
                return data.get('last_txn_id', 0)
    except Exception as e:
        logger.error(f"Error loading state file: {e}")
    return 0


def save_last_txn_id(txn_id):
    state_file = get_state_file_path()
    try:
        os.makedirs(os.path.dirname(state_file) or '.', exist_ok=True)
        with open(state_file, 'w') as f:
            json.dump({'last_txn_id': txn_id, 'updated_at': time.time()}, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving state file: {e}")


def get_table_columns(conn, table_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COLUMN_NAME
                FROM ALL_TAB_COLUMNS
                WHERE TABLE_NAME = UPPER(:table_name)
                  AND OWNER = 'RPT'
                ORDER BY COLUMN_ID
            """, table_name=table_name)
            columns = [row[0] for row in cursor.fetchall()]
            logger.info(f"📋 Columns in {table_name}: {columns}")
            return columns
    except Exception as e:
        logger.error(f"Error getting columns for {table_name}: {e}")
        return []


def insert_sms(cursor, bank_id, mobile, message, txn_date=None, client_id=None,
               card_no=None, amount=None, currency='BDT'):
    """Insert into SMS_Q_TABLE"""
    cursor.execute("""
        INSERT INTO RPT.SMS_Q_TABLE
            (TXN_DATE, BANK_ID, CLIENT_ID, CARD_NO, MOBILE, MESSAGE, 
             AMOUNT, CURRENCY, MSG_TYPE, STATUS, RETRY_COUNT)
        VALUES
            (:txn_date, :bank_id, :client_id, :card_no, :mobile, :message,
             :amount, :currency, 'PAYMENT', 'P', 0)
    """,
        txn_date=txn_date,
        bank_id=str(bank_id).strip(),
        client_id=client_id,
        card_no=str(card_no) if card_no else None,
        mobile=str(mobile),
        message=message,
        amount=amount,
        currency=currency
    )


def insert_email(cursor, bank_id, email, message, subject=None, txn_date=None,
                 client_id=None, card_no=None, amount=None, currency='BDT'):
    """Insert into EMAIL_Q_TABLE"""
    if not email:
        return
    cursor.execute("""
        INSERT INTO RPT.EMAIL_Q_TABLE
            (TXN_DATE, BANK_ID, CLIENT_ID, CARD_NO, EMAIL, SUBJECT, MESSAGE,
             AMOUNT, CURRENCY, STATUS, RETRY_COUNT)
        VALUES
            (:txn_date, :bank_id, :client_id, :card_no, :email, :subject, :message,
             :amount, :currency, 'P', 0)
    """,
        txn_date=txn_date,
        bank_id=str(bank_id).strip(),
        client_id=client_id,
        card_no=str(card_no) if card_no else None,
        email=str(email),
        subject=subject or 'Payment Notification',
        message=message,
        amount=amount,
        currency=currency
    )


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

            get_table_columns(conn, 'SMS_Q_TABLE')
            get_table_columns(conn, 'EMAIL_Q_TABLE')
            get_table_columns(conn, 'PAYMENT_TEST')

except Exception as e:
    logger.error(f"❌ Oracle Connection FAILED: {e}")
    exit(1)

# ====================== Create Connection Pool ======================
logger.info("Creating Oracle Connection Pool...")
pool = oracledb.create_pool(
    user=oracle_cfg['user'],
    password=oracle_cfg['password'],
    dsn=oracle_cfg['dsn'],
    min=5,
    max=20,
    increment=2
)

logger.info("🚀 Payment Producer Started - Polling every 5 minutes...")

# ====================== Main Polling Loop ======================
records_inserted = 0
poll_count = 0
last_txn_id = load_last_txn_id()
logger.info(f"📌 Loaded last transaction ID: {last_txn_id}")

POLL_INTERVAL = 300  # 5 minutes

while True:
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                               SELECT TXN_ID,
                                      MOBILE,
                                      EMAIL,
                                      AMOUNT,
                                      BANK_ID,
                                      MESSAGE,
                                      CURRENCY,
                                      CREATED_AT -- Changed from TXN_DATE
                               FROM RPT.PAYMENT_TEST
                               WHERE TXN_ID > :last_txn_id
                               ORDER BY TXN_ID ASC
                                   FETCH FIRST 1000 ROWS ONLY
                               """, last_txn_id=last_txn_id)

                rows = cursor.fetchall()

                if rows:
                    poll_count += 1
                    logger.info(f"📊 Poll #{poll_count} - Found {len(rows)} new payment records")

                    max_txn_id = last_txn_id
                    sms_count = 0
                    email_count = 0

                    for row in rows:
                        try:
                            txn_id, mobile, email, amount, bank_id, message, currency, created_at = row

                            bank = next((b for b in config['banks']
                                         if str(b['bank_id']).strip() == str(bank_id).strip()), None)

                            if not bank:
                                logger.warning(f"Bank ID {bank_id} not found for TXN {txn_id}")
                                max_txn_id = max(max_txn_id, txn_id)
                                continue

                            sms_message = message or f"Payment of {amount} {currency or 'BDT'} processed successfully"

                            # ── SMS ──────────────────────────────────────────
                            if mobile:
                                insert_sms(
                                    cursor,
                                    bank_id,
                                    mobile,
                                    sms_message,
                                    txn_date=created_at,  # Use CREATED_AT
                                    client_id='1235',
                                    card_no='4567890123456789',
                                    amount=amount,
                                    currency=currency or 'BDT'
                                )
                                sms_count += 1

                            # ── EMAIL ─────────────────────────────────────────
                            if email:
                                insert_email(
                                    cursor,
                                    bank_id,
                                    email,
                                    sms_message,
                                    subject=f"Payment Notification - TXN {txn_id}",
                                    txn_date=created_at,  # Use CREATED_AT
                                    client_id='1235',
                                    card_no='4567890123456789',
                                    amount=amount,
                                    currency=currency or 'BDT'
                                )
                                email_count += 1

                            max_txn_id = max(max_txn_id, txn_id)

                        except Exception as e:
                            logger.error(f"Error processing TXN {txn_id}: {e}")
                            continue

                    conn.commit()

                    if max_txn_id > last_txn_id:
                        last_txn_id = max_txn_id
                        save_last_txn_id(last_txn_id)

                    records_inserted += sms_count + email_count
                    logger.info(f"✅ SMS: {sms_count} | Email: {email_count} | Total: {records_inserted}")
                    logger.info(f"💾 Last TXN ID saved: {last_txn_id}")

                else:
                    logger.debug("No new payment records found")

        time.sleep(POLL_INTERVAL)

    except Exception as e:
        logger.error(f"Payment Producer Error: {e}", exc_info=True)
        time.sleep(10)