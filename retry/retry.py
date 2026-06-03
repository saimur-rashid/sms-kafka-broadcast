from utils import load_config, create_consumer, create_producer, update_sms_status
from utils.logger_config import setup_logger
import json
import time
import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = setup_logger('retry', 'retry')

config = load_config()
consumer = create_consumer("retry-group")
consumer.subscribe([config['kafka']['retry_topic']])

logger.info("🚀 Retry Service Started...")

# ✅ INCREASED to 100 workers
executor = ThreadPoolExecutor(max_workers=config['kafka'].get('retry_max_workers', 100))


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


batch_messages = []
last_process_time = time.time()

while True:
    try:
        msg = consumer.poll(0.05)
        if msg is not None and not msg.error():
            batch_messages.append(msg)

        # Process when: 500 messages OR 1 second passed
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

                    future = executor.submit(call_ulka_api, bank, data)
                    futures_to_msg[future] = (msg, data)
                except Exception as e:
                    logger.error(f"Retry prep error: {e}")

            for future in as_completed(futures_to_msg.keys()):
                msg, data = futures_to_msg[future]
                msg_id = data.get('id')
                retry_count = int(data.get('retry_count', 0))

                try:
                    success = future.result()

                    if success:
                        logger.info(f"✅ RETRY SUCCESS - {msg_id}")
                        update_sms_status(msg_id, status="S", retry_count=retry_count)
                    else:
                        retry_count += 1
                        data['retry_count'] = retry_count
                        p = create_producer()

                        if retry_count >= config['kafka']['max_retries']:
                            logger.error(f"🚨 DLQ: {msg_id}")
                            p.produce(config['kafka']['dlq_topic'], json.dumps(data).encode('utf-8'))
                            update_sms_status(msg_id, status="F", retry_count=retry_count,
                                              error_msg="Max retries exhausted")
                        else:
                            p.produce(config['kafka']['retry_topic'], json.dumps(data).encode('utf-8'))
                            update_sms_status(msg_id, status="R", retry_count=retry_count,
                                              error_msg="Retry attempt")
                        p.flush()

                    consumer.commit(msg)
                except Exception as e:
                    logger.error(f"Retry result error: {e}")

            logger.info(f"✅ Retry batch complete. Waiting 2 seconds...")
            time.sleep(2)
            batch_messages = []
            last_process_time = time.time()

    except Exception as e:
        logger.error(f"Retry error: {e}", exc_info=True)
        time.sleep(1)