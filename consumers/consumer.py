import json
import time
import requests
import xml.etree.ElementTree as ET
from utils import load_config, create_producer, create_consumer, logger, update_sms_status

config = load_config()
kafka_cfg = config['kafka']

#consumer = create_consumer("sms-bank-consumer-v2")
# Dynamic consumer group for testing (starts fresh every time)
consumer_group = f"sms-bank-consumer-{int(time.time())}"
consumer = create_consumer(consumer_group)
consumer.subscribe([kafka_cfg['sms_topic']])

logger.info(f"🚀 Consumer started with group: {consumer_group}")

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

        logger.info(f"🌐 Calling ULKA: {payload['id']} -> {bank['bank_id']}")

        response = requests.get(url, timeout=15)
        logger.debug(f"ULKA Raw Response: {response.text}")

        # Parse XML response
        root = ET.fromstring(response.text)
        response_code = root.findtext("ResponseCode")
        info = root.findtext("Info")

        logger.info(f"ULKA Response - Code: {response_code} | Info: {info}")

        return response_code == "111"   # Success code

    except requests.exceptions.Timeout:
        logger.error(f"ULKA Timeout - ID: {payload.get('id')}")
        return False
    except requests.exceptions.ConnectionError:
        logger.error(f"ULKA Connection Error - ID: {payload.get('id')}")
        return False
    except ET.ParseError as e:
        logger.error(f"Invalid XML from ULKA: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error calling ULKA: {e}")
        return False


while True:
    try:
        msg = consumer.poll(1.0)          # Reduced poll time
        if msg is None:
            continue

        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue

        payload = json.loads(msg.value().decode('utf-8'))
        bank_id = payload['bank_id']
        msg_id = payload["id"]
        retry_count = int(payload.get("retry_count", 0))

        # Find bank config
        bank = next((b for b in config['banks']
                    if str(b['bank_id']).strip() == str(bank_id).strip()), None)

        if not bank:
            logger.error(f"Bank {bank_id} not found in config")
            consumer.commit(msg)
            continue

        success = call_ulka_api(bank, payload)

        if success:
            logger.info(f"✅ SUCCESS - ID: {msg_id} | Bank: {bank_id}")
            update_sms_status(msg_id, status="S", retry_count=retry_count)
        else:
            retry_count += 1
            payload['retry_count'] = retry_count
            payload['error'] = "ULKA API Failed"

            p = create_producer()

            if retry_count >= kafka_cfg['max_retries']:
                logger.error(f"🚨 Max Retries Exceeded - Moving to DLQ: {msg_id}")
                p.produce(kafka_cfg['dlq_topic'], json.dumps(payload).encode('utf-8'))
                update_sms_status(msg_id, status="F", retry_count=retry_count,
                                error_msg="Max retries exhausted")
            else:
                logger.warning(f"🔄 Retry {retry_count}/{kafka_cfg['max_retries']} - ID: {msg_id}")
                p.produce(kafka_cfg['retry_topic'], json.dumps(payload).encode('utf-8'))
                update_sms_status(msg_id, status="R", retry_count=retry_count,
                                error_msg="ULKA API Failed")

            p.flush()

        consumer.commit(msg)

    except Exception as e:
        logger.error(f"Consumer processing error: {e}", exc_info=True)
        time.sleep(1)