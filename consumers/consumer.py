import json
import time
import requests
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import load_config, create_producer, create_consumer, update_sms_status
from utils.logger_config import setup_logger

logger = setup_logger('consumer', 'consumer')
config = load_config()
kafka_cfg = config['kafka']

consumer_group = f"sms-bank-consumer-{int(time.time())}"
consumer = create_consumer(consumer_group)
consumer.subscribe([kafka_cfg['sms_topic']])

logger.info(f"🚀 Consumer started with group: {consumer_group}")

# ✅ INCREASED to 200 workers for 10K msg/sec
executor = ThreadPoolExecutor(max_workers=kafka_cfg.get('consumer_max_workers', 200))


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
success_count = 0
failed_count = 0

while True:
    try:
        # Collect messages in batch
        msg = consumer.poll(0.05)  # ← Reduced timeout for faster batch collection
        if msg is not None and not msg.error():
            batch_messages.append(msg)

        # Process when: 1000 messages collected OR 1 second passed
        should_process = (
                len(batch_messages) >= 1000 or
                (len(batch_messages) > 0 and time.time() - last_process_time >= 1)
        )

        if should_process and batch_messages:
            logger.info(f"📦 Processing batch: {len(batch_messages)} messages...")
            start_time = time.time()

            futures_to_msg = {}

            # Submit all requests in parallel
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

            # Process results as they complete
            for future in as_completed(futures_to_msg.keys()):
                msg, payload = futures_to_msg[future]
                msg_id = payload["id"]
                bank_id = payload['bank_id']
                retry_count = int(payload.get("retry_count", 0))

                try:
                    success = future.result()

                    if success:
                        update_sms_status(msg_id, status="S", retry_count=retry_count)
                        success_count += 1
                    else:
                        retry_count += 1
                        payload['retry_count'] = retry_count
                        p = create_producer()

                        if retry_count >= kafka_cfg['max_retries']:
                            logger.error(f"🚨 DLQ: {msg_id}")
                            p.produce(kafka_cfg['dlq_topic'], json.dumps(payload).encode('utf-8'))
                            update_sms_status(msg_id, status="F", retry_count=retry_count,
                                              error_msg="Max retries exhausted")
                        else:
                            logger.warning(f"🔄 Retry {retry_count}/{kafka_cfg['max_retries']} - {msg_id}")
                            p.produce(kafka_cfg['retry_topic'], json.dumps(payload).encode('utf-8'))
                            update_sms_status(msg_id, status="R", retry_count=retry_count,
                                              error_msg="ULKA API Failed")
                        p.flush()
                        failed_count += 1

                    consumer.commit(msg)

                except Exception as e:
                    logger.error(f"Result error - ID: {payload.get('id')}: {e}")

            elapsed = time.time() - start_time
            batch_size = len(batch_messages)
            throughput = batch_size / elapsed if elapsed > 0 else 0

            logger.info(f"✅ Batch complete: {batch_size} msgs in {elapsed:.2f}s ({throughput:.0f} msg/sec)")
            logger.info(f"📊 Success: {success_count} | Failed: {failed_count}")

            # ✅ WAIT 2 SECONDS AFTER BATCH COMPLETE
            logger.info(f"⏳ Waiting 2 seconds before next batch...")
            time.sleep(2)
            batch_messages = []
            last_process_time = time.time()

    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)
        time.sleep(1)