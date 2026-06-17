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
APP_STATE_FILE = 'last_txn_state.json'  # Fallback for local testing


def get_state_file_path():
    """Get the correct state file path"""
    if os.path.exists('/app/payment'):
        return STATE_FILE
    return APP_STATE_FILE


def load_last_txn_id():
    """Load last transaction ID from state file"""
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
    """Save last transaction ID to state file"""
    state_file = get_state_file_path()
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(state_file) or '.', exist_ok=True)
        with open(state_file, 'w') as f:
            json.dump({'last_txn_id': txn_id, 'updated_at': time.time()}, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving state file: {e}")


def get_table_columns(conn, table_name):
    """Helper function to get column names from a table"""
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

            # Check table structure
            get_table_columns(conn, 'SMS_EMAIL_Q_TABLE')
            get_table_columns(conn, 'PAYMENT_TEST')

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
    min=5,
    max=20,
    increment=2
)

logger.info("🚀 Payment Producer Started - Polling Payment Table every 5 minutes...")

# ====================== Main Polling Loop ======================
records_inserted = 0
poll_count = 0
last_txn_id = load_last_txn_id()
logger.info(f"📌 Loaded last transaction ID: {last_txn_id}")

POLL_INTERVAL = 300  # 5 minutes in seconds

while True:
    try:
        with pool.acquire() as conn:
            with conn.cursor() as cursor:
                # ✅ Select all columns
                cursor.execute("""
                               SELECT TXN_ID, MOBILE, EMAIL, AMOUNT, BANK_ID, MESSAGE
                               FROM rpt.PAYMENT_TEST
                               WHERE TXN_ID > :last_txn_id
                               ORDER BY TXN_ID ASC
                                   FETCH FIRST 1000 ROWS ONLY
                               """, last_txn_id=last_txn_id)

                rows = cursor.fetchall()

                if rows:
                    logger.info(f"📊 Poll #{poll_count} - Found {len(rows)} new payment records")
                    poll_count += 1

                    max_txn_id = last_txn_id
                    inserted_count = 0

                    for row in rows:
                        try:
                            # ✅ Unpack all 6 columns
                            txn_id, mobile, email, amount, bank_id, message = row

                            logger.debug(
                                f"Processing TXN {txn_id}: mobile={mobile}, email={email}, amount={amount}, bank_id={bank_id}")

                            # Skip if bank_id not in config
                            bank = next((b for b in config['banks']
                                         if str(b['bank_id']).strip() == str(bank_id).strip()), None)

                            if not bank:
                                logger.warning(f"Bank ID {bank_id} not found in config for TXN {txn_id}")
                                max_txn_id = max(max_txn_id, txn_id)
                                continue

                            # Build message if not provided
                            sms_message = message or f"Payment of {amount} processed successfully"

                            # ✅ Option 1: If STATUS column exists but with different name
                            # Try different variations of the INSERT
                            try:
                                # First try with STATUS column (as originally written)
                                cursor.execute("""
                                               INSERT INTO rpt.SMS_EMAIL_Q_TABLE
                                               (BANK_ID, MESSAGE, MOBILE, EMAIL, STATUS, MSG_TYPE, RETRY_COUNT,
                                                CREATE_AT, UPDATED_AT)
                                               VALUES (:bank_id, :message, :mobile, :email, 'P', 'PAYMENT', 0, SYSDATE,
                                                       SYSDATE)
                                               """,
                                               bank_id=str(bank_id).strip(),
                                               message=sms_message,
                                               mobile=str(mobile),
                                               email=str(email) if email else None)

                            except oracledb.DatabaseError as e:
                                error_msg = str(e)
                                if "ORA-00904" in error_msg and "STATUS" in error_msg:
                                    logger.warning(f"STATUS column not found, trying without STATUS")
                                    # ✅ Option 2: Try without STATUS column
                                    cursor.execute("""
                                                   INSERT INTO rpt.SMS_EMAIL_Q_TABLE
                                                   (BANK_ID, MESSAGE, MOBILE, EMAIL, MSG_TYPE, RETRY_COUNT, CREATE_AT,
                                                    UPDATED_AT)
                                                   VALUES (:bank_id, :message, :mobile, :email, 'PAYMENT', 0, SYSDATE,
                                                           SYSDATE)
                                                   """,
                                                   bank_id=str(bank_id).strip(),
                                                   message=sms_message,
                                                   mobile=str(mobile),
                                                   email=str(email) if email else None)
                                elif "ORA-00904" in error_msg and "MSG_TYPE" in error_msg:
                                    logger.warning(f"MSG_TYPE column not found, trying without MSG_TYPE")
                                    # ✅ Option 3: Try without MSG_TYPE column
                                    cursor.execute("""
                                                   INSERT INTO rpt.SMS_EMAIL_Q_TABLE
                                                       (BANK_ID, MESSAGE, MOBILE, EMAIL, RETRY_COUNT, CREATE_AT, UPDATED_AT)
                                                   VALUES (:bank_id, :message, :mobile, :email, 0, SYSDATE, SYSDATE)
                                                   """,
                                                   bank_id=str(bank_id).strip(),
                                                   message=sms_message,
                                                   mobile=str(mobile),
                                                   email=str(email) if email else None)
                                else:
                                    raise e

                            inserted_count += 1
                            max_txn_id = max(max_txn_id, txn_id)
                            logger.debug(f"✉️ Inserted SMS/Email for payment TXN {txn_id}")

                        except Exception as e:
                            logger.error(f"Error processing payment TXN {row[0] if row else 'unknown'}: {e}")
                            # Don't break the loop, continue with next record
                            continue

                    # Commit the transaction
                    conn.commit()

                    # Update last processed transaction ID
                    if max_txn_id > last_txn_id:
                        last_txn_id = max_txn_id
                        save_last_txn_id(last_txn_id)

                    records_inserted += inserted_count
                    logger.info(f"✅ Inserted {inserted_count} SMS/Email records | Total inserted: {records_inserted}")
                    logger.info(f"💾 Saved last transaction ID: {last_txn_id}")
                    logger.info(f"⏳ Next poll in 5 minutes...")

                else:
                    logger.debug("No new payment records found")

        # Wait 5 minutes before next poll
        time.sleep(POLL_INTERVAL)

    except Exception as e:
        logger.error(f"Payment Producer Error: {e}", exc_info=True)
        time.sleep(5)  # Wait 5 seconds before retry on error