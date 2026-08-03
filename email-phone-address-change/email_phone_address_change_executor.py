import json
import time
import os
import oracledb

from utils import load_config
from utils.logger_config import setup_logger


# ============================================================
# LOGGER
# ============================================================

logger = setup_logger(
    'client_field_change_executor',
    'client_field_change_executor'
)


# ============================================================
# CONFIG
# ============================================================

config = load_config()

oracle_cfg = config['oracle']


# ============================================================
# STATE FILE
# ============================================================

STATE_FILE = '/app/email-phone-address-change/last_change_state.json'
LOCAL_STATE_FILE = 'last_change_state.json'


def get_state_file_path():

    if os.path.exists('/app/email-phone-address-change'):
        return STATE_FILE

    return LOCAL_STATE_FILE


# ============================================================
# LOAD LAST PROCESSED ID
# ============================================================

def load_last_processed_id():

    state_file = get_state_file_path()

    try:

        if os.path.exists(state_file):

            with open(state_file, 'r') as f:

                data = json.load(f)

                last_id = int(
                    data.get(
                        'last_processed_id',
                        0
                    )
                )

                logger.info(
                    f"📌 Last processed change ID: {last_id}"
                )

                return last_id

    except Exception as e:

        logger.error(
            f"❌ Error loading state file: {e}"
        )

    return 0


# ============================================================
# SAVE LAST PROCESSED ID
# ============================================================

def save_last_processed_id(change_id):

    state_file = get_state_file_path()

    try:

        os.makedirs(
            os.path.dirname(state_file) or '.',
            exist_ok=True
        )

        temp_file = state_file + '.tmp'

        data = {
            'last_processed_id': change_id,
            'updated_at': time.time()
        }

        with open(temp_file, 'w') as f:

            json.dump(
                data,
                f,
                indent=2
            )

        os.replace(
            temp_file,
            state_file
        )

        logger.info(
            f"💾 Saved last processed ID: {change_id}"
        )

    except Exception as e:

        logger.error(
            f"❌ Error saving state: {e}"
        )


# ============================================================
# FIND CURRENT CUSTOMER CONTACT INFORMATION
# ============================================================

def get_current_customer_info(
        cursor,
        client_id
):

    """
    IMPORTANT:
    Replace OWS.CLIENT with your actual
    customer master table/view.

    Must return:
        MOBILE
        EMAIL
    """

    cursor.execute(
        """
        SELECT
            PHONE_M,
            E_MAIL
        FROM OWS.CLIENT
        WHERE ID = :client_id
        """,
        client_id=client_id
    )

    row = cursor.fetchone()

    if not row:

        logger.warning(
            f"⚠️ Client not found: {client_id}"
        )

        return None, None

    mobile, email = row

    return mobile, email


# ============================================================
# INSERT SMS
# ============================================================

def insert_sms(
        cursor,
        txn_date,
        bank_id,
        client_id,
        mobile,
        message,
        msg_type
):

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
            MOBILE,
            MESSAGE,
            MSG_TYPE,
            STATUS,
            RETRY_COUNT,
            CREATED_AT,
            UPDATED_AT
        )
        VALUES
        (
            :txn_date,
            :bank_id,
            :client_id,
            :mobile,
            :message,
            :msg_type,
            'P',
            0,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        )
        """,

        txn_date=txn_date,

        bank_id=str(
            bank_id
        ).strip(),

        client_id=client_id,

        mobile=str(
            mobile
        ).strip(),

        message=message,

        msg_type=msg_type
    )

    logger.info(
        f"📱 SMS inserted "
        f"| Client={client_id} "
        f"| Mobile={mobile}"
    )

    return True


# ============================================================
# INSERT EMAIL
# ============================================================

def insert_email(
        cursor,
        txn_date,
        bank_id,
        client_id,
        email,
        subject,
        message
):

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
            EMAIL,
            SUBJECT,
            MESSAGE,
            STATUS,
            RETRY_COUNT,
            CREATED_AT,
            UPDATED_AT
        )
        VALUES
        (
            :txn_date,
            :bank_id,
            :client_id,
            :email,
            :subject,
            :message,
            'P',
            0,
            SYSTIMESTAMP,
            SYSTIMESTAMP
        )
        """,
        txn_date=txn_date,
        bank_id=str(bank_id).strip(),
        client_id=client_id,
        email=str(email).strip(),
        subject=subject,
        message=message
    )

    logger.info(
        f"📧 Email inserted "
        f"| Client={client_id} "
        f"| Email={email}"
    )

    return True


# ============================================================
# PROCESS MOBILE CHANGE
# ============================================================

def process_mobile_change(
        cursor,
        change_id,
        client_id,
        bank_id,
        txn_date,
        old_value,
        new_value,
        current_email
):

    logger.info(
        f"📱 Processing MOBILE change "
        f"| ID={change_id} "
        f"| Client={client_id}"
    )

    message = (
        f"Dear Customer, your mobile number changed "
        f"from {old_value} to {new_value}. "
        f"If you did not request this change, "
        f"please contact your bank immediately."
    )

    # ========================================================
    # SMS 1 - OLD MOBILE
    # ========================================================

    if old_value:

        insert_sms(
            cursor=cursor,
            txn_date=txn_date,
            bank_id=bank_id,
            client_id=client_id,
            mobile=old_value,
            message=message,
            msg_type='CUSTOMER_INFO_CHANGE'
        )

    # ========================================================
    # SMS 2 - NEW MOBILE
    # ========================================================

    if new_value:

        # Avoid duplicate if old and new are same
        if str(old_value).strip() != str(new_value).strip():

            insert_sms(
                cursor=cursor,
                txn_date=txn_date,
                bank_id=bank_id,
                client_id=client_id,
                mobile=new_value,
                message=message,
                msg_type='CUSTOMER_INFO_CHANGE'
            )

    # ========================================================
    # EMAIL - CURRENT EMAIL
    # ========================================================

    email_message = (
        f"Client ID: {client_id} |"
        f"Field Updated: Mobile Number |"
        f"Previous Mobile Number: {old_value or 'N/A'} |"
        f"New Mobile Number: {new_value or 'N/A'} |"
        f"Updated On: {txn_date}"
    )

    if current_email:

        insert_email(
            cursor=cursor,
            txn_date=txn_date,
            bank_id=bank_id,
            client_id=client_id,
            email=current_email,
            subject='Mobile Number Change Notification',
            message=email_message
        )


# ============================================================
# PROCESS EMAIL CHANGE
# ============================================================

def process_email_change(
        cursor,
        change_id,
        client_id,
        bank_id,
        txn_date,
        old_value,
        new_value,
        current_mobile
):

    logger.info(
        f"📧 Processing EMAIL change "
        f"| ID={change_id} "
        f"| Client={client_id}"
    )

    message = (
        f"Client ID: {client_id} |"
        f"Field Updated: Email ID |"
        f"Previous Email: {old_value or 'N/A'} |"
        f"New Email: {new_value or 'N/A'} |"
        f"Updated On: {txn_date}"
    )

    # ========================================================
    # SMS - CURRENT MOBILE
    # ========================================================

    sms_message = (
        f"Dear Customer, your email ID changed "
        f"from {old_value} to {new_value}. "
        f"If you did not request this change, "
        f"please contact your bank immediately."
    )

    if current_mobile:

        insert_sms(
            cursor=cursor,
            txn_date=txn_date,
            bank_id=bank_id,
            client_id=client_id,
            mobile=current_mobile,
            message=sms_message,
            msg_type='CUSTOMER_INFO_CHANGE'
        )

    # ========================================================
    # EMAIL 1 - OLD EMAIL
    # ========================================================

    if old_value:

        insert_email(
            cursor=cursor,
            txn_date=txn_date,
            bank_id=bank_id,
            client_id=client_id,
            email=old_value,
            subject='CUSTOMER_INFO_CHANGE Notification',
            message=message
        )

    # ========================================================
    # EMAIL 2 - NEW EMAIL
    # ========================================================

    if new_value:

        if str(old_value).strip() != str(new_value).strip():

            insert_email(
                cursor=cursor,
                txn_date=txn_date,
                bank_id=bank_id,
                client_id=client_id,
                email=new_value,
                subject='CUSTOMER_INFO_CHANGE Notification',
                message=message
            )


# ============================================================
# PROCESS ADDRESS CHANGE
# ============================================================

def process_address_change(
        cursor,
        change_id,
        client_id,
        bank_id,
        txn_date,
        old_value,
        new_value,
        current_mobile,
        current_email
):

    logger.info(
        f"🏠 Processing ADDRESS change "
        f"| ID={change_id} "
        f"| Client={client_id}"
    )

    message = (
        f"Dear Customer, your address changed "
        f"from {old_value} to {new_value}. "
        f"If you did not request this change, "
        f"please contact your bank immediately."
    )

    # ========================================================
    # SMS - CURRENT MOBILE
    # ========================================================

    if current_mobile:

        insert_sms(
            cursor=cursor,
            txn_date=txn_date,
            bank_id=bank_id,
            client_id=client_id,
            mobile=current_mobile,
            message=message,
            msg_type='CUSTOMER_INFO_CHANGE'
        )

    # ========================================================
    # EMAIL - CURRENT EMAIL
    # ========================================================
    email_message = (
        f"Client ID: {client_id} |"
        f"Field Updated: Address |"
        f"Previous Address: {old_value or 'N/A'} |"
        f"New Address: {new_value or 'N/A'} |"
        f"Updated On: {txn_date}"
    )

    if current_email:

        insert_email(
            cursor=cursor,
            txn_date=txn_date,
            bank_id=bank_id,
            client_id=client_id,
            email=current_email,
            subject='CUSTOMER_INFO_CHANGE Notification',
            message=email_message
        )


# ============================================================
# PROCESS ONE CHANGE LOG RECORD
# ============================================================

def process_change(
        cursor,
        row
):

    (
        change_id,
        client_id,
        field_name,
        old_value,
        new_value,
        changed_by,
        changed_at,
        app_context,
        bank_id
    ) = row


    field_name = (
        str(field_name).strip().upper()
        if field_name
        else ''
    )


    logger.info(
        f"🔄 Change detected "
        f"| ID={change_id} "
        f"| Client={client_id} "
        f"| Field={field_name} "
        f"| Bank={bank_id}"
    )


    # ========================================================
    # GET CURRENT CUSTOMER CONTACT
    # ========================================================

    current_mobile, current_email = get_current_customer_info(
        cursor,
        client_id
    )


    # ========================================================
    # TXN_DATE
    # ========================================================

    txn_date = changed_at


    # ========================================================
    # MOBILE CHANGE
    # ========================================================

    mobile_fields = {

        'MOBILE',

        'PHONE',

        'PHONE_M',

        'PHONE_H'
    }


    if field_name in mobile_fields:

        process_mobile_change(

            cursor=cursor,

            change_id=change_id,

            client_id=client_id,

            bank_id=bank_id,

            txn_date=txn_date,

            old_value=old_value,

            new_value=new_value,

            current_email=current_email
        )

        return True


    # ========================================================
    # EMAIL CHANGE
    # ========================================================

    email_fields = {

        'EMAIL',

        'E_MAIL',

        'EMAIL_ID',

        'EMAIL_ADDRESS'
    }


    if field_name in email_fields:

        process_email_change(

            cursor=cursor,

            change_id=change_id,

            client_id=client_id,

            bank_id=bank_id,

            txn_date=txn_date,

            old_value=old_value,

            new_value=new_value,

            current_mobile=current_mobile
        )

        return True


    # ========================================================
    # ADDRESS CHANGE
    # ========================================================

    address_fields = {

        'ADDRESS',

        'ADDRESS_LINE_1',

        'ADDRESS_LINE_2',

        'ADDRESS_LINE_3',

        'CITY',

        'DISTRICT',

        'POSTAL_CODE',

        'ZIP_CODE'
    }


    if field_name in address_fields:

        process_address_change(

            cursor=cursor,

            change_id=change_id,

            client_id=client_id,

            bank_id=bank_id,

            txn_date=txn_date,

            old_value=old_value,

            new_value=new_value,

            current_mobile=current_mobile,

            current_email=current_email
        )

        return True


    # ========================================================
    # UNKNOWN FIELD
    # ========================================================

    logger.info(
        f"ℹ️ No notification rule for "
        f"FIELD_NAME={field_name} "
        f"| ID={change_id}"
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
# CONNECTION POOL
# ============================================================

logger.info(
    "🔧 Creating Oracle Connection Pool..."
)


pool = oracledb.create_pool(

    user=oracle_cfg['user'],

    password=oracle_cfg['password'],

    dsn=oracle_cfg['dsn'],

    min=2,

    max=10,

    increment=1
)


# ============================================================
# LOAD STATE
# ============================================================

last_processed_id = load_last_processed_id()


# ============================================================
# MAIN LOOP
# ============================================================

logger.info(
    "🚀 Client Field Change Executor Started"
)


while True:

    try:

        with pool.acquire() as conn:

            with conn.cursor() as cursor:

                # =================================================
                # GET NEW CHANGE LOG RECORDS
                # =================================================

                cursor.execute(
                    """
                    SELECT
                        ID,
                        CLIENT_ID,
                        FIELD_NAME,
                        OLD_VALUE,
                        NEW_VALUE,
                        CHANGED_BY,
                        CHANGED_AT,
                        APP_CONTEXT,
                        F_I
                    FROM ows.CLIENT_FIELD_CHANGE_LOG
                    WHERE ID > :last_id
                    ORDER BY ID ASC
                    FETCH FIRST 1000 ROWS ONLY
                    """,

                    last_id=last_processed_id
                )


                rows = cursor.fetchall()


                if not rows:

                    logger.info(
                        f"ℹ️ No new changes "
                        f"| Last ID={last_processed_id}"
                    )


                else:

                    logger.info(
                        f"📦 Found {len(rows)} new changes"
                    )


                    # Process sequentially
                    for row in rows:

                        change_id = row[0]


                        try:

                            process_change(

                                cursor,

                                row
                            )


                            # Commit each change
                            # so successful changes
                            # are permanently queued

                            conn.commit()


                            # Update state only after
                            # successful DB commit

                            last_processed_id = change_id


                            save_last_processed_id(

                                last_processed_id
                            )


                            logger.info(

                                f"✅ Change ID={change_id} "
                                f"completed successfully"
                            )


                        except Exception as e:

                            conn.rollback()


                            logger.error(

                                f"❌ Change ID={change_id} failed: {e}",

                                exc_info=True
                            )


                            # IMPORTANT:
                            # Stop here.
                            # Do NOT process ID+1.
                            #
                            # This guarantees that if ID=5
                            # fails, next execution will again
                            # start with ID=5.

                            break


        # =========================================================
        # WAIT
        # =========================================================

        time.sleep(10)


    except KeyboardInterrupt:

        logger.info(
            "🛑 Application stopped"
        )

        break


    except Exception as e:

        logger.error(

            f"❌ Main process error: {e}",

            exc_info=True
        )

        time.sleep(10)