import requests
import xml.etree.ElementTree as ET
import logging
from urllib.parse import quote

# ==========================
# Logging Configuration
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("ULKA_TEST")

# ==========================
# ULKA Configuration
# ==========================
ulka = {
    "base_url": "http://10.0.14.83:6841",
    "source_id": "BAL_TXN",
    "username": "bal_txn",
    "password": "Bal@6841",
    "sender": "QCASH"
}

# ==========================
# Test Payload
# ==========================
payload = {
    "phone": "01729603778",      # <-- Replace with your number
    "message": "Hello from ULKA API Test"
}

# ==========================
# Build URL
# ==========================
encoded_message = quote(payload["message"])

url = (
    f"{ulka['base_url'].rstrip('/')}/sendSMS?"
    f"sourceId={ulka['source_id']}&"
    f"userName={ulka['username']}&"
    f"password={ulka['password']}&"
    f"sender={ulka['sender']}&"
    f"msisdn={payload['phone']}&"
    f"message={encoded_message}"
)

logger.info("=" * 80)
logger.info("Sending SMS Request")
logger.info("URL: %s", url)
logger.info("=" * 80)

try:
    response = requests.get(url, timeout=15)

    logger.info("HTTP Status : %s", response.status_code)
    logger.info("Response Headers : %s", response.headers)
    logger.info("Response Body:\n%s", response.text)

    if response.status_code != 200:
        logger.error("HTTP Request Failed")
        exit()

    try:
        root = ET.fromstring(response.text)

        logger.info("XML Parsed Successfully")

        for child in root.iter():
            logger.info("%s = %s", child.tag, child.text)

        response_code = root.findtext("ResponseCode")

        logger.info("ResponseCode : %s", response_code)

        if response_code == "111":
            logger.info("✅ SMS SENT SUCCESSFULLY")
        else:
            logger.error("❌ SMS FAILED")

    except ET.ParseError:
        logger.error("Response is NOT valid XML")
        logger.info("Raw Response:\n%s", response.text)

except Exception as e:
    logger.exception("Exception occurred")