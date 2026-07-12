import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import load_config, create_producer, create_consumer, update_email_status
from utils.logger_config import setup_logger

logger = setup_logger('consumer_email', 'consumer_email')
config = load_config()
kafka_cfg = config['kafka']

# FIX #1: Fixed group ID — time-based group resets offsets on every restart
consumer_group = "email-bank-consumer"
consumer = create_consumer(consumer_group)
consumer.subscribe([kafka_cfg['email_topic']])

logger.info(f"🚀 Email Consumer started with group: {consumer_group}")

# FIX #2: Single producer created once at startup — not per failed message
retry_producer = create_producer()

executor = ThreadPoolExecutor(max_workers=kafka_cfg.get('consumer_max_workers', 200))


def call_email_api(bank, payload):
    """Call Internal Email API Gateway"""
    try:
        email_api_url = config['email_api']['url']
        api_key = config['email_api']['api_key']

        # Build the required "text" field
        text_body = (
            f"txn_type: {payload.get('SUBJECT', payload.get('msg_type', 'Transaction'))} | "
            f"txn_no : {payload.get('id', 'N/A')} | "
            f"card_no : {payload.get('card_no', 'N/A')} | "
            f"txn_date: {payload.get('txn_date', 'N/A')} | "
            f"currency : {payload.get('currency', 'N/A')} | "
            f"amount : {payload.get('amount', 'N/A')}"
        )

        email_payload = {
            "from": bank.get('email_from', "notifications@yourbank.com"),
            "to": [payload['email']],
            "cc": [],
            "bcc": [],
            "subject": payload.get('subject') or bank.get('email_subject', 'Payment Notification'),
            "date": time.strftime("%Y-%m-%d %I:%M %p", time.gmtime()),
            "text": text_body
        }

        # ==================== DEBUG PRINT ====================
        print("\n" + "="*80)
        print("EMAIL PAYLOAD TO BE SENT:")
        print("="*80)
        print(json.dumps(email_payload, indent=2, ensure_ascii=False))
        print("="*80 + "\n")

        # Optional: Also log it at DEBUG level
        logger.debug(f"Full Email Payload:\n{json.dumps(email_payload, indent=2)}")

        headers = {
            'Content-Type': 'application/json',
            'secret-key': api_key
        }

        logger.info(f"Sending email to {payload['email']} | ID: {payload.get('id')}")

        response = requests.post(
            email_api_url,
            json=email_payload,
            headers=headers,
            timeout=30
        )

        logger.info(f"Email API Response | Status={response.status_code} | ID={payload.get('id')}")

        if response.status_code in [200, 201, 202]:
            logger.debug(f"✅ Email sent successfully for ID: {payload.get('id')}")
            return True, response.json() if response.text else {}
        else:
            logger.error(f"Email API Failed | Status={response.status_code} | Body={response.text[:400]}")
            return False, {"error": f"HTTP {response.status_code}", "response": response.text}

    except Exception as e:
        logger.exception(f"Email API Exception - ID: {payload.get('id')}")
        return False, {"error": str(e)}


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
            logger.info(f"📦 Processing email batch: {len(batch_messages)} messages...")
            start_time = time.time()

            futures_to_msg = {}

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

            for future in as_completed(futures_to_msg.keys()):
                msg, payload = futures_to_msg[future]
                msg_id      = payload["id"]
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

                        if retry_count >= kafka_cfg['max_retries']:
                            logger.error(f"🚨 Email DLQ: {msg_id}")
                            retry_producer.produce(
                                kafka_cfg['dlq_topic'],
                                json.dumps(payload).encode('utf-8')
                            )
                            update_email_status(msg_id, status="F", retry_count=retry_count,
                                                error_msg="Max retries exhausted",
                                                response_data=json.dumps(response_data))
                        else:
                            logger.warning(
                                f"🔄 Email Retry {retry_count}/{kafka_cfg['max_retries']} - {msg_id}"
                            )
                            retry_producer.produce(
                                kafka_cfg['retry_topic'],
                                json.dumps(payload).encode('utf-8')
                            )
                            update_email_status(msg_id, status="R", retry_count=retry_count,
                                                error_msg="Email API Failed",
                                                response_data=json.dumps(response_data))

                        retry_producer.flush()
                        failed_count += 1

                    consumer.commit(msg)

                except Exception as e:
                    logger.error(f"Email result error - ID: {payload.get('id')}: {e}")

            elapsed    = time.time() - start_time
            batch_size = len(batch_messages)
            throughput = batch_size / elapsed if elapsed > 0 else 0

            logger.info(
                f"✅ Email batch complete: {batch_size} msgs in {elapsed:.2f}s "
                f"({throughput:.0f} msg/sec)"
            )
            logger.info(f"📊 Email Success: {success_count} | Failed: {failed_count}")
            logger.info("⏳ Waiting 2 seconds before next email batch...")

            time.sleep(2)
            batch_messages = []
            last_process_time = time.time()

    except Exception as e:
        logger.error(f"Email Consumer error: {e}", exc_info=True)
        time.sleep(1)