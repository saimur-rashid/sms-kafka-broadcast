import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import load_config, create_producer, create_consumer, update_email_status
from utils.logger_config import setup_logger

logger = setup_logger('consumer_email', 'consumer_email')
config = load_config()
kafka_cfg = config['kafka']

consumer_group = f"email-bank-consumer-{int(time.time())}"
consumer = create_consumer(consumer_group)
consumer.subscribe([kafka_cfg['email_topic']])

logger.info(f"🚀 Email Consumer started with group: {consumer_group}")

# ✅ Workers for email processing
executor = ThreadPoolExecutor(max_workers=kafka_cfg.get('consumer_max_workers', 200))


def call_email_api(bank, payload):
    """Call Email API Gateway"""
    try:
        email_api_url = config['email_api']['url']
        api_key = config['email_api']['api_key']
        email_timeout = config['email_api'].get('timeout', 30)

        # Get email configuration from bank
        email_from = bank.get('email_from')
        email_subject = bank.get('email_subject')

        # Prepare email payload
        email_payload = {
            "from": email_from,
            "to": [payload['email']],  # Single recipient
            "cc": [],
            "bcc": [],
            "subject": email_subject,
            "date": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "body": payload['message']
        }

        headers = {
            'Content-Type': 'application/json',
            'secret-key': api_key  # As per your requirement
        }

        logger.debug(f"Sending email to {payload['email']} for ID: {payload.get('id')}")

        response = requests.post(
            email_api_url,
            json=email_payload,
            headers=headers,
            timeout=30
        )

        if response.status_code in [200, 201, 202]:
            logger.debug(f"✅ Email sent successfully for ID: {payload.get('id')}")
            return True, response.json() if response.text else {}
        else:
            logger.error(f"Email API returned {response.status_code}: {response.text}")
            return False, {"error": f"HTTP {response.status_code}", "response": response.text}

    except requests.exceptions.Timeout:
        logger.error(f"Email API timeout for ID: {payload.get('id')}")
        return False, {"error": "Timeout"}
    except requests.exceptions.ConnectionError:
        logger.error(f"Email API connection error for ID: {payload.get('id')}")
        return False, {"error": "Connection Error"}
    except Exception as e:
        logger.error(f"Email API Error - ID: {payload.get('id')}: {e}")
        return False, {"error": str(e)}


batch_messages = []
last_process_time = time.time()
success_count = 0
failed_count = 0

while True:
    try:
        # Collect messages in batch
        msg = consumer.poll(0.05)
        if msg is not None and not msg.error():
            batch_messages.append(msg)

        # Process when: 1000 messages collected OR 1 second passed
        should_process = (
                len(batch_messages) >= 1000 or
                (len(batch_messages) > 0 and time.time() - last_process_time >= 1)
        )

        if should_process and batch_messages:
            logger.info(f"📦 Processing email batch: {len(batch_messages)} messages...")
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
                        logger.error(f"Bank {bank_id} not found for email ID: {payload.get('id')}")
                        consumer.commit(msg)
                        continue

                    # Validate email address
                    if not payload.get('email'):
                        logger.error(f"Email address missing for ID: {payload.get('id')}")
                        update_email_status(payload["id"], status="F",
                                            error_msg="Email address missing")
                        consumer.commit(msg)
                        continue

                    future = executor.submit(call_email_api, bank, payload)
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
                    success, response_data = future.result()

                    if success:
                        update_email_status(msg_id, status="S", retry_count=retry_count,
                                            response_data=json.dumps(response_data))
                        success_count += 1
                    else:
                        retry_count += 1
                        payload['retry_count'] = retry_count
                        p = create_producer()

                        if retry_count >= kafka_cfg['max_retries']:
                            logger.error(f"🚨 Email DLQ: {msg_id}")
                            p.produce(kafka_cfg['dlq_topic'],
                                      json.dumps(payload).encode('utf-8'))
                            update_email_status(msg_id, status="F", retry_count=retry_count,
                                                error_msg="Max retries exhausted",
                                                response_data=json.dumps(response_data))
                        else:
                            logger.warning(f"🔄 Email Retry {retry_count}/{kafka_cfg['max_retries']} - {msg_id}")
                            p.produce(kafka_cfg['retry_topic'],
                                      json.dumps(payload).encode('utf-8'))
                            update_email_status(msg_id, status="R", retry_count=retry_count,
                                                error_msg="Email API Failed",
                                                response_data=json.dumps(response_data))
                        p.flush()
                        failed_count += 1

                    consumer.commit(msg)

                except Exception as e:
                    logger.error(f"Email result error - ID: {payload.get('id')}: {e}")

            elapsed = time.time() - start_time
            batch_size = len(batch_messages)
            throughput = batch_size / elapsed if elapsed > 0 else 0

            logger.info(f"✅ Email batch complete: {batch_size} msgs in {elapsed:.2f}s ({throughput:.0f} msg/sec)")
            logger.info(f"📊 Email Success: {success_count} | Failed: {failed_count}")

            # ✅ WAIT 2 SECONDS AFTER BATCH COMPLETE
            logger.info(f"⏳ Waiting 2 seconds before next email batch...")
            time.sleep(2)
            batch_messages = []
            last_process_time = time.time()

    except Exception as e:
        logger.error(f"Email Consumer error: {e}", exc_info=True)
        time.sleep(1)