import json
import time
import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import load_config, create_consumer, create_producer, update_sms_status, update_email_status
from utils.logger_config import setup_logger

logger = setup_logger('retry', 'retry')
config = load_config()
kafka_cfg = config['kafka']

# FIX #1: Fixed group ID — time-based group resets offsets on every restart
consumer = create_consumer("retry-group")
consumer.subscribe([kafka_cfg['retry_topic']])

# FIX #2: Single producer at startup — not per failed message inside the loop
retry_producer = create_producer()

executor = ThreadPoolExecutor(max_workers=kafka_cfg.get('retry_max_workers', 100))

logger.info("🚀 Retry Service Started...")


def call_ulka_api(bank, payload):
    """Call ULKA SMS Gateway"""
    try:
        ulka = bank['ulka']
        base_url = ulka['base_url'].rstrip('/')
        encoded_message = requests.utils.quote(payload['message'])

        url = (
            f"{base_url}/sendSMS?"
            f"sourceId={ulka['source_id']}&"
            f"userName={ulka['username']}&"
            f"password={ulka['password']}&"
            f"sender={ulka['sender']}&"
            f"msisdn={payload['phone']}&"
            f"message={encoded_message}"
        )

        response = requests.get(url, timeout=15)
        root = ET.fromstring(response.text)
        response_code = root.findtext("ResponseCode")
        return response_code == "111"

    except Exception as e:
        logger.error(f"ULKA Error - ID: {payload.get('id')}: {e}")
        return False


def call_email_api(bank, payload):
    """Call Email API Gateway"""
    try:
        email_api_url = config['email_api']['url']
        api_key = config['email_api']['api_key']

        email_payload = {
            "from":    bank.get('email_from'),
            "to":      [payload['email']],
            "cc":      [],
            "bcc":     [],
            "subject": payload.get('subject') or bank.get('email_subject', 'Notification'),
            "date":    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "body":    payload['message']
        }

        headers = {
            'Content-Type': 'application/json',
            'secret-key': api_key
        }

        response = requests.post(email_api_url, json=email_payload, headers=headers, timeout=30)
        return response.status_code in [200, 201, 202]

    except Exception as e:
        logger.error(f"Email Error - ID: {payload.get('id')}: {e}")
        return False


def dispatch_api_call(bank, payload):
    """FIX #3: Route to correct API based on msg_type in payload."""
    msg_type = payload.get('msg_type', 'SMS')
    if msg_type == 'EMAIL':
        return call_email_api(bank, payload)
    return call_ulka_api(bank, payload)


def update_status(msg_id, msg_type, status, retry_count, error_msg=None):
    """FIX #3: Route status update to correct table based on msg_type."""
    if msg_type == 'EMAIL':
        update_email_status(msg_id, status=status, retry_count=retry_count, error_msg=error_msg)
    else:
        update_sms_status(msg_id, status=status, retry_count=retry_count, error_msg=error_msg)


batch_messages = []
last_process_time = time.time()

while True:
    try:
        msg = consumer.poll(0.05)
        if msg is not None and not msg.error():
            batch_messages.append(msg)

        should_process = (
            len(batch_messages) >= 500 or
            (len(batch_messages) > 0 and time.time() - last_process_time >= 1)
        )

        if should_process and batch_messages:
            logger.info(f"📦 Retry batch: {len(batch_messages)} messages...")

            futures_to_msg = {}

            for msg in batch_messages:
                try:
                    data = json.loads(msg.value().decode())
                    bank_id = data.get('bank_id')
                    bank = next((b for b in config['banks']
                                 if str(b['bank_id']).strip() == str(bank_id).strip()), None)

                    if not bank:
                        logger.error(f"Bank {bank_id} not found")
                        consumer.commit(msg)
                        continue

                    future = executor.submit(dispatch_api_call, bank, data)
                    futures_to_msg[future] = (msg, data)

                except Exception as e:
                    logger.error(f"Retry prep error: {e}")
                    consumer.commit(msg)

            for future in as_completed(futures_to_msg.keys()):
                msg, data = futures_to_msg[future]
                msg_id      = data.get('id')
                msg_type    = data.get('msg_type', 'SMS')
                retry_count = int(data.get('retry_count', 0))

                try:
                    success = future.result()

                    if success:
                        logger.info(f"✅ RETRY SUCCESS [{msg_type}] - {msg_id}")
                        update_status(msg_id, msg_type, status="S", retry_count=retry_count)
                    else:
                        retry_count += 1
                        data['retry_count'] = retry_count

                        if retry_count >= kafka_cfg['max_retries']:
                            logger.error(f"🚨 DLQ [{msg_type}]: {msg_id}")
                            retry_producer.produce(
                                kafka_cfg['dlq_topic'],
                                json.dumps(data).encode('utf-8')
                            )
                            update_status(msg_id, msg_type, status="F",
                                          retry_count=retry_count,
                                          error_msg="Max retries exhausted")
                        else:
                            logger.warning(
                                f"🔄 Re-queuing [{msg_type}] "
                                f"{retry_count}/{kafka_cfg['max_retries']} - {msg_id}"
                            )
                            retry_producer.produce(
                                kafka_cfg['retry_topic'],
                                json.dumps(data).encode('utf-8')
                            )
                            update_status(msg_id, msg_type, status="R",
                                          retry_count=retry_count,
                                          error_msg="Retry attempt")

                        retry_producer.flush()

                    consumer.commit(msg)

                except Exception as e:
                    logger.error(f"Retry result error: {e}")

            logger.info("✅ Retry batch complete. Waiting 2 seconds...")
            time.sleep(2)
            batch_messages = []
            last_process_time = time.time()

    except Exception as e:
        logger.error(f"Retry error: {e}", exc_info=True)
        time.sleep(1)