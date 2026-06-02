import os
import json
import logging

import oracledb
from confluent_kafka import Producer, Consumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_config():
    # Get the directory where this utils.py file is located
    utils_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(utils_dir, 'config.json')

    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def create_producer():
    config = load_config()
    bootstrap_servers = ','.join(config['kafka']['bootstrap_servers'])
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'sms-producer',
        'acks': 'all',
        'retries': 1,
    }
    return Producer(conf)


def create_consumer(group_id):
    config = load_config()
    bootstrap_servers = ','.join(config['kafka']['bootstrap_servers'])
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Changed from 'latest' to reprocess messages
        'enable.auto.commit': False,
    }
    return Consumer(conf)


# ─── Oracle DB Pool ──────────────────────────────────────────────────────────
_pool: oracledb.ConnectionPool | None = None


def get_pool(config: dict | None = None) -> oracledb.ConnectionPool:
    """Returns a shared connection pool (lazy-initialised, singleton)."""
    global _pool
    if _pool is None:
        if config is None:
            config = load_config()
        oracle_cfg = config["oracle"]
        _pool = oracledb.create_pool(
            user=oracle_cfg["user"],
            password=oracle_cfg["password"],
            dsn=oracle_cfg["dsn"],
            min=2,
            max=10,
            increment=1,
        )
    return _pool


# ─── DB Status Updater ───────────────────────────────────────────────────────
# Status codes:
#   P = Pending   (initial, sitting in DB)
#   Q = Queued    (pulled by producer, pushed to Kafka)
#   S = Sent      (consumer got 200/202 from ULKA)
#   R = Retry     (consumer failed, pushed to retry topic)
#   F = Failed    (exhausted retries, moved to DLQ)

def update_sms_status(
        msg_id,
        status,
        retry_count=0,
        error_msg=None,
        consumer_id=None,
        config=None,
):
    pool = get_pool(config)

    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE SMS_Q_TABLE
                SET STATUS        = :status,
                    RETRY_COUNT   = :retry_count,
                    ERROR_MESSAGE = :error_msg,
                    CONSUMER_ID   = :consumer_id,
                    UPDATED_AT    = SYSTIMESTAMP
                WHERE ID = :msg_id
                """,
                status=status,
                retry_count=retry_count,
                error_msg=error_msg,
                consumer_id=consumer_id,
                msg_id=msg_id,
            )

        conn.commit()