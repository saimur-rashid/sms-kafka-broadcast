import json
import time
import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import load_config, create_producer, create_consumer, update_sms_status, update_email_status
from utils.logger_config import setup_logger

logger = setup_logger('consumer_sms', 'consumer_sms')
config = load_config()
kafka_cfg = config['kafka']

# FIX #3: Fixed group ID — a time-based group resets offsets on every restart,
# meaning messages get replayed or skipped unpredictably.
consumer_group = "sms-bank-consumer"
consumer = create_consumer(consumer_group)
consumer.subscribe([kafka_cfg['sms_topic']])

logger.info(f"🚀 Consumer started with group: {consumer_group}")

# FIX #2: Single producer created once — not per failed message inside the loop.
# Creating a producer per failure at 10K msg/sec creates thousands of connections.
retry_producer = create_producer()

executor = ThreadPoolExecutor(max_workers=kafka_cfg.get('consumer_max_workers', 200))


def call_ulka_api(bank, payload):
    try:
        ulka = bank["ulka"]
        base_url = ulka["base_url"].rstrip("/")
        encoded_message = requests.utils.quote(payload["message"])

        url = (
            f"{base_url}/sendSMS?"
            f"sourceId={ulka['source_id']}&"
            f"userName={ulka['username']}&"
            f"password={ulka['password']}&"
            f"sender={ulka['sender']}&"
            f"msisdn={payload['phone']}&"
            f"message={encoded_message}"
        )

        logger.info(f"SMS ID={payload['id']}")
        logger.info(f"URL={url}")

        response = requests.get(url, timeout=15)

        logger.info(f"HTTP={response.status_code}")
        logger.info(f"BODY={response.text}")

        root = ET.fromstring(response.text)
        response_code = root.findtext("ResponseCode")
        logger.info(f"ResponseCode={response_code}")

        return response_code == "111"

    except Exception:
        logger.exception("ULKA API Exception")
        return False


def update_status(msg_id, msg_type, status, retry_count, error_msg=None):
    """
    FIX #1: Route status updates to the correct table based on msg_type.
    msg_type 'SMS'   → SMS_Q_TABLE   via update_sms_status()
    msg_type 'EMAIL' → EMAIL_Q_TABLE via update_email_status()
    Defaults to SMS if msg_type is missing (backward compatibility).
    """
    if msg_type == 'EMAIL':
        update_email_status(msg_id, status=status, retry_count=retry_count, error_msg=error_msg)
    else:
        update_sms_status(msg_id, status=status, retry_count=retry_count, error_msg=error_msg)


batch_messages = []
last_process_time = time.time()
success_count = 0
failed_count = 0

while True:
    try:
        msg = consumer.poll(0.05)
        if msg is not None and not msg.error():
            batch_messages.append(msg)

        should_process = (
            len(batch_messages) >= 1000 or
            (len(batch_messages) > 0 and time.time() - last_process_time >= 1)
        )

        if should_process and batch_messages:
            logger.info(f"📦 Processing batch: {len(batch_messages)} messages...")
            start_time = time.time()

            futures_to_msg = {}

            for msg in batch_messages:
                try:
                    payload = json.loads(msg.value().decode('utf-8'))
                    bank_id = payload['bank_id']
                    bank = next((b for b in config['banks']
                                 if str(b['bank_id']).strip() == str(bank_id).strip()), None)

                    if not bank:
                        logger.error(f"Bank {bank_id} not found")
                        consumer.commit(msg)
                        continue

                    future = executor.submit(call_ulka_api, bank, payload)
                    futures_to_msg[future] = (msg, payload)

                except Exception as e:
                    logger.error(f"Batch prep error: {e}")
                    consumer.commit(msg)

            for future in as_completed(futures_to_msg.keys()):
                msg, payload = futures_to_msg[future]
                msg_id     = payload["id"]
                bank_id    = payload['bank_id']
                msg_type   = payload.get("msg_type", "SMS")   # 'SMS' or 'EMAIL'
                retry_count = int(payload.get("retry_count", 0))

                try:
                    success = future.result()

                    if success:
                        update_status(msg_id, msg_type, status="S", retry_count=retry_count)
                        success_count += 1
                    else:
                        retry_count += 1
                        payload['retry_count'] = retry_count

                        if retry_count >= kafka_cfg['max_retries']:
                            logger.error(f"🚨 DLQ: {msg_id}")
                            retry_producer.produce(
                                kafka_cfg['dlq_topic'],
                                json.dumps(payload).encode('utf-8')
                            )
                            update_status(msg_id, msg_type, status="F",
                                          retry_count=retry_count,
                                          error_msg="Max retries exhausted")
                        else:
                            logger.warning(f"🔄 Retry {retry_count}/{kafka_cfg['max_retries']} - {msg_id}")
                            retry_producer.produce(
                                kafka_cfg['retry_topic'],
                                json.dumps(payload).encode('utf-8')
                            )
                            update_status(msg_id, msg_type, status="R",
                                          retry_count=retry_count,
                                          error_msg="ULKA API Failed")

                        retry_producer.flush()
                        failed_count += 1

                    consumer.commit(msg)

                except Exception as e:
                    logger.error(f"Result error - ID: {payload.get('id')}: {e}")

            elapsed = time.time() - start_time
            batch_size = len(batch_messages)
            throughput = batch_size / elapsed if elapsed > 0 else 0

            logger.info(f"✅ Batch complete: {batch_size} msgs in {elapsed:.2f}s ({throughput:.0f} msg/sec)")
            logger.info(f"📊 Success: {success_count} | Failed: {failed_count}")
            logger.info("⏳ Waiting 2 seconds before next batch...")

            time.sleep(2)
            batch_messages = []
            last_process_time = time.time()

    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)
        time.sleep(1)