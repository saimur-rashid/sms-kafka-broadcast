from utils import load_config, create_consumer, create_producer, update_sms_status
from utils.logger_config import setup_logger
import json
import time
import requests
import xml.etree.ElementTree as ET

logger = setup_logger('retry', 'retry')

config = load_config()
consumer = create_consumer("retry-group")
consumer.subscribe([config['kafka']['retry_topic']])

logger.info("🚀 Retry Service Started - Processing failed messages...")

def call_ulka_api(bank, payload):
    """Call ULKA SMS Gateway - Same as consumer"""
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

        logger.info(f"🔄 Retrying ULKA: {payload['id']} -> {bank['bank_id']} (Attempt {payload.get('retry_count', 0) + 1}/3)")

        response = requests.get(url, timeout=15)
        logger.info(f"ULKA Response Status: {response.status_code}")

        # Parse XML response
        root = ET.fromstring(response.text)
        response_code = root.findtext("ResponseCode")
        info = root.findtext("Info")

        logger.info(f"ULKA Response - Code: {response_code} | Info: {info}")

        return response_code == "111"  # Success code

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
        logger.error(f"Unexpected error calling ULKA: {e}", exc_info=True)
        return False


while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        data = json.loads(msg.value().decode())
        msg_id = data.get('id')
        bank_id = data.get('bank_id')
        retry_count = int(data.get('retry_count', 0))

        # Find bank config
        bank = next((b for b in config['banks']
                    if str(b['bank_id']).strip() == str(bank_id).strip()), None)

        if not bank:
            logger.error(f"Bank {bank_id} not found in config")
            consumer.commit(msg)
            continue

        # Try the API call again
        success = call_ulka_api(bank, data)

        if success:
            logger.info(f"✅ RETRY SUCCESS - ID: {msg_id} | Bank: {bank_id}")
            update_sms_status(msg_id, status="S", retry_count=retry_count)
        else:
            # Still failing - increment retry count
            retry_count += 1
            data['retry_count'] = retry_count

            if retry_count >= config['kafka']['max_retries']:
                logger.error(f"🚨 Max Retries Exceeded - Moving to DLQ: {msg_id}")
                p = create_producer()
                p.produce(config['kafka']['dlq_topic'], json.dumps(data).encode('utf-8'))
                p.flush()
                update_sms_status(msg_id, status="F", retry_count=retry_count,
                                error_msg="Max retries exhausted")
            else:
                logger.warning(f"🔄 Retry {retry_count}/{config['kafka']['max_retries']} - Republishing: {msg_id}")
                p = create_producer()
                p.produce(config['kafka']['retry_topic'], json.dumps(data).encode('utf-8'))
                p.flush()
                update_sms_status(msg_id, status="R", retry_count=retry_count,
                                error_msg="Retry attempt")

        consumer.commit(msg)

    except Exception as e:
        logger.error(f"Retry service error: {e}", exc_info=True)
        time.sleep(1)