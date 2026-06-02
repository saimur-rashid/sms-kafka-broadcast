import json
from utils import load_config, create_consumer, logger

config = load_config()
kafka_cfg = config['kafka']

consumer = create_consumer("dlq-group")
consumer.subscribe([kafka_cfg['dlq_topic']])

logger.info("🚨 DLQ Consumer Started - Waiting for failed messages...")

while True:
    msg = consumer.poll(2.0)
    if msg is None:
        continue
    if msg.error():
        logger.error(f"DLQ Error: {msg.error()}")
        continue

    try:
        data = json.loads(msg.value().decode('utf-8'))

        logger.critical(f"""
        ═══════════════════════════════════════
        🛑 DLQ ALERT - FAILED MESSAGE
        ID          : {data.get('id')}
        Bank ID     : {data.get('bank_id')}
        Phone       : {data.get('phone')}
        Retry Count : {data.get('retry_count', 0)}
        Error       : {data.get('error', 'Unknown')}
        Time        : {data.get('timestamp')}
        ═══════════════════════════════════════
        """)

        # Optional: Save to file for manual review
        with open('/app/dlq_messages.log', 'a', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False) + '\n')

    except Exception as e:
        logger.error(f"Failed to process DLQ message: {e}")

    consumer.commit(msg)