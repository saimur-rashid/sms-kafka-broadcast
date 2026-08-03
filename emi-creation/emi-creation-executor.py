import time
import oracledb

from utils import load_config
from utils.logger_config import setup_logger


# ============================================================
# LOGGER
# ============================================================

logger = setup_logger(
    'emi_payment_executor',
    'emi_payment_executor'
)


# ============================================================
# CONFIG
# ============================================================

config = load_config()

oracle_cfg = config['oracle']


# ============================================================
# DUMMY EMI SQL
# ============================================================

def get_dummy_emi_data(cursor):
    """
    TEMPORARY DUMMY SQL

    Later replace this SQL with your actual EMI query.

    This returns exactly 1 dummy EMI record.
    """

    cursor.execute(
        """
        SELECT
            1234 AS CLIENT_ID,
            'XXXX1234' AS CARD_NO,
            50000 AS TRANSACTION_AMOUNT,
            TO_DATE(
                '28/07/2026 12:30',
                'DD/MM/YYYY HH24:MI'
            ) AS TRANSACTION_DATE,
            'ABC Electronics' AS MERCHANT_NAME,
            5000 AS EMI_AMOUNT,
            12 AS TENURE,
            416.67 AS MONTHLY_EMI_AMOUNT,
            TO_DATE(
                '28/08/2026',
                'DD/MM/YYYY'
            ) AS FIRST_EMI_DATE,
            '1790' AS BANK_ID,
            '01700000000' AS MOBILE,
            'customer@example.com' AS EMAIL
        FROM DUAL
        """
    )

    row = cursor.fetchone()

    return row


# ============================================================
# BUILD MESSAGE
# ============================================================

def build_emi_message(
        client_id,
        card_no,
        transaction_amount,
        transaction_date,
        merchant_name,
        emi_amount,
        tenure,
        monthly_emi_amount,
        first_emi_date
):
    """
    Create the complete EMI notification message.

    This same message will be inserted into:

        RPT.SMS_Q_TABLE.MESSAGE

    and

        RPT.EMAIL_Q_TABLE.MESSAGE
    """

    transaction_date_text = (
        transaction_date.strftime(
            '%d/%m/%Y, %H:%M'
        )
        if transaction_date
        else 'N/A'
    )

    first_emi_date_text = (
        first_emi_date.strftime(
            '%d/%m/%Y'
        )
        if first_emi_date
        else 'N/A'
    )

    message = (
        f"Client ID: {client_id} |"
        f"Card Number: {card_no or 'N/A'} |"
        f"Transaction Amount: {transaction_amount or 'N/A'} |"
        f"Transaction Date: {transaction_date_text} |"
        f"Merchant Name: {merchant_name or 'N/A'} |"
        f"EMI Amount: {emi_amount or 'N/A'} |"
        f"Tenure: {tenure or 'N/A'} |"
        f"Monthly EMI Amount: {monthly_emi_amount or 'N/A'} |"
        f"First EMI Date: {first_emi_date_text}"
    )

    return message


# ============================================================
# INSERT SMS
# ============================================================

def insert_sms(
        cursor,
        bank_id,
        client_id,
        card_no,
        mobile,
        message,
        txn_date,
        amount
):
    """
    Insert EMI notification into SMS_Q_TABLE.
    """

    if not mobile:

        logger.warning(
            f"⚠️ SMS skipped. Mobile is NULL "
            f"| Client={client_id}"
        )

        return False

    cursor.execute(
        """
        INSERT INTO RPT.SMS_Q_TABLE
        (
            TXN_DATE,
            BANK_ID,
            CLIENT_ID,
            CARD_NO,
            MOBILE,
            MESSAGE,
            MSG_TYPE,
            STATUS,
            RETRY_COUNT,
            CURRENCY,
            AMOUNT,
            CREATED_AT,
            UPDATED_AT
        )
        VALUES
        (
            :txn_date,
            :bank_id,
            :client_id,
            :card_no,
            :mobile,
            :message,
            'EMI_TRANSACTION',
            'P',
            0,
            'BDT',
            :amount,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        )
        """,

        txn_date=txn_date,

        bank_id=str(
            bank_id
        ).strip(),

        client_id=client_id,

        card_no=(
            str(card_no).strip()
            if card_no
            else None
        ),

        mobile=str(
            mobile
        ).strip(),

        message=message,

        amount=amount
    )

    logger.info(
        f"📱 EMI SMS inserted "
        f"| Client={client_id} "
        f"| Mobile={mobile}"
    )

    return True


# ============================================================
# INSERT EMAIL
# ============================================================

def insert_email(
        cursor,
        bank_id,
        client_id,
        card_no,
        email,
        subject,
        message,
        txn_date,
        amount
):
    """
    Insert EMI notification into EMAIL_Q_TABLE.
    """

    if not email:

        logger.warning(
            f"⚠️ Email skipped. Email is NULL "
            f"| Client={client_id}"
        )

        return False

    cursor.execute(
        """
        INSERT INTO RPT.EMAIL_Q_TABLE
        (
            TXN_DATE,
            BANK_ID,
            CLIENT_ID,
            CARD_NO,
            EMAIL,
            SUBJECT,
            MESSAGE,
            STATUS,
            RETRY_COUNT,
            CURRENCY,
            AMOUNT,
            CREATED_AT,
            UPDATED_AT
        )
        VALUES
        (
            :txn_date,
            :bank_id,
            :client_id,
            :card_no,
            :email,
            :subject,
            :message,
            'P',
            0,
            'BDT',
            :amount,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        )
        """,

        txn_date=txn_date,

        bank_id=str(
            bank_id
        ).strip(),

        client_id=client_id,

        card_no=(
            str(card_no).strip()
            if card_no
            else None
        ),

        email=str(
            email
        ).strip(),

        subject=subject,

        message=message,

        amount=amount
    )

    logger.info(
        f"📧 EMI Email inserted "
        f"| Client={client_id} "
        f"| Email={email}"
    )

    return True


# ============================================================
# ORACLE CONNECTION TEST
# ============================================================

logger.info(
    "🔍 Testing Oracle Database Connection..."
)

try:

    with oracledb.connect(

        user=oracle_cfg['user'],

        password=oracle_cfg['password'],

        dsn=oracle_cfg['dsn']

    ) as conn:

        with conn.cursor() as cursor:

            cursor.execute(
                "SELECT SYSDATE FROM DUAL"
            )

            result = cursor.fetchone()

            logger.info(
                f"✅ Oracle Connection Successful "
                f"| Server Time={result[0]}"
            )


except Exception as e:

    logger.error(
        f"❌ Oracle Connection FAILED: {e}"
    )

    exit(1)


# ============================================================
# CREATE CONNECTION POOL
# ============================================================

logger.info(
    "🔧 Creating Oracle Connection Pool..."
)

pool = oracledb.create_pool(

    user=oracle_cfg['user'],

    password=oracle_cfg['password'],

    dsn=oracle_cfg['dsn'],

    min=1,

    max=5,

    increment=1
)


# ============================================================
# MAIN EXECUTION
# ============================================================

logger.info(
    "🚀 EMI Notification Executor Started"
)


try:

    with pool.acquire() as conn:

        with conn.cursor() as cursor:

            # =================================================
            # FETCH ONE DUMMY EMI RECORD
            # =================================================

            row = get_dummy_emi_data(
                cursor
            )

            if not row:

                logger.info(
                    "ℹ️ No EMI record found"
                )

            else:

                (
                    client_id,
                    card_no,
                    transaction_amount,
                    transaction_date,
                    merchant_name,
                    emi_amount,
                    tenure,
                    monthly_emi_amount,
                    first_emi_date,
                    bank_id,
                    mobile,
                    email
                ) = row


                logger.info(
                    f"📦 EMI record found "
                    f"| Client={client_id} "
                    f"| Card={card_no}"
                )


                # =================================================
                # BUILD FULL EMI MESSAGE
                # =================================================

                message = build_emi_message(

                    client_id=client_id,

                    card_no=card_no,

                    transaction_amount=transaction_amount,

                    transaction_date=transaction_date,

                    merchant_name=merchant_name,

                    emi_amount=emi_amount,

                    tenure=tenure,

                    monthly_emi_amount=monthly_emi_amount,

                    first_emi_date=first_emi_date
                )


                # =================================================
                # PRINT MESSAGE FOR TESTING
                # =================================================

                print()
                print("=" * 70)
                print("EMI NOTIFICATION MESSAGE")
                print("=" * 70)
                print(message)
                print("=" * 70)
                print()


                # =================================================
                # INSERT SMS
                # =================================================

                sms_inserted = insert_sms(

                    cursor=cursor,

                    bank_id=bank_id,

                    client_id=client_id,

                    card_no=card_no,

                    mobile=mobile,

                    message=message,

                    txn_date=transaction_date,

                    amount=transaction_amount
                )


                # =================================================
                # INSERT EMAIL
                # =================================================

                email_inserted = insert_email(

                    cursor=cursor,

                    bank_id=bank_id,

                    client_id=client_id,

                    card_no=card_no,

                    email=email,

                    subject=(
                        'EMI Transaction Notification'
                    ),

                    message=message,

                    txn_date=transaction_date,

                    amount=transaction_amount
                )


                # =================================================
                # COMMIT
                # =================================================

                conn.commit()


                logger.info(
                    f"✅ EMI processing completed "
                    f"| SMS={sms_inserted} "
                    f"| Email={email_inserted}"
                )


except Exception as e:

    logger.error(
        f"❌ EMI processing failed: {e}",
        exc_info=True
    )

    try:

        conn.rollback()

    except Exception:

        pass


finally:

    try:

        pool.close()

        logger.info(
            "🔒 Oracle Connection Pool Closed"
        )

    except Exception as e:

        logger.error(
            f"Error closing pool: {e}"
        )


logger.info(
    "🏁 EMI Notification Executor Finished"
)

