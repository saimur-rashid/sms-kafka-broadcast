import json
import time
from utils import load_config, create_consumer, update_sms_status, update_email_status
from utils.logger_config import setup_logger

logger = setup_logger('dlq', 'dlq')
config = load_config()
kafka_cfg = config['kafka']

# Fixed group ID (was already fixed in original — keeping as-is)
consumer = create_consumer("dlq-group")
consumer.subscribe([kafka_cfg['dlq_topic']])

logger.info("🚨 DLQ Consumer Started - Waiting for failed messages...")

DLQ_LOG_FILE = '/app/dlq_messages.log'


def update_status_final(msg_id, msg_type, retry_count, error_msg="Max retries exhausted"):
    """Mark record as permanently failed (F) in the correct table."""
    if msg_type == 'EMAIL':
        update_email_status(msg_id, status="F", retry_count=retry_count, error_msg=error_msg)
    else:
        update_sms_status(msg_id, status="F", retry_count=retry_count, error_msg=error_msg)


while True:
    msg = consumer.poll(2.0)
    if msg is None:
        continue
    if msg.error():
        logger.error(f"DLQ Error: {msg.error()}")
        continue

    try:
        data = json.loads(msg.value().decode('utf-8'))

        msg_id      = data.get('id')
        msg_type    = data.get('msg_type', 'SMS')   # 'SMS' or 'EMAIL'
        bank_id     = data.get('bank_id')
        retry_count = int(data.get('retry_count', 0))

        # Build log details based on type
        if msg_type == 'EMAIL':
            contact_line = f"Email       : {data.get('email')}"
        else:
            contact_line = f"Phone       : {data.get('phone')}"

        logger.critical(f"""
        ═══════════════════════════════════════
        🛑 DLQ ALERT - FAILED MESSAGE
        Type        : {msg_type}
        ID          : {msg_id}
        Bank ID     : {bank_id}
        {contact_line}
        Retry Count : {retry_count}
        Error       : {data.get('error', 'Unknown')}
        Time        : {data.get('timestamp', time.strftime('%Y-%m-%dT%H:%M:%SZ'))}
        ═══════════════════════════════════════
        """)

        # Mark as permanently failed in the correct DB table
        update_status_final(msg_id, msg_type, retry_count)

        # Append to DLQ log file for manual review
        with open(DLQ_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False) + '\n')

    except Exception as e:
        logger.error(f"Failed to process DLQ message: {e}")

    consumer.commit(msg)