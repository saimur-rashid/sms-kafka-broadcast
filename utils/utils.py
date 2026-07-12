import os
import json
import logging

import oracledb
from confluent_kafka import Producer, Consumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_config():
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
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }
    return Consumer(conf)


# ─── Oracle DB Pool ──────────────────────────────────────────────────────────
_pool: oracledb.ConnectionPool | None = None


def get_pool(config: dict | None = None) -> oracledb.ConnectionPool:
    """Shared connection pool — lazy-initialised singleton."""
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
        # Optional: Add LOB handler globally
        oracledb.defaults.fetch_lobs = False
    return _pool


# ─── DB Status Updaters ───────────────────────────────────────────────────────

def update_sms_status(
        msg_id,
        status,
        retry_count=0,
        error_msg=None,
        consumer_id=None,
        response_data=None,
        config=None,
):
    """Update STATUS on RPT.SMS_Q_TABLE"""
    pool = get_pool(config)
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE RPT.SMS_Q_TABLE
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
    logger.debug(f"SMS_Q_TABLE updated: ID={msg_id} STATUS={status}")


def update_email_status(
        msg_id,
        status,
        retry_count=0,
        error_msg=None,
        consumer_id=None,
        response_data=None,
        config=None,
):
    """Update STATUS on RPT.EMAIL_Q_TABLE"""
    pool = get_pool(config)
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE RPT.EMAIL_Q_TABLE
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
    logger.debug(f"EMAIL_Q_TABLE updated: ID={msg_id} STATUS={status}")


# ─── Get Message Status ───────────────────────────────────────────────────────

def get_message_status(msg_id, msg_type="sms"):
    """Return current STATUS from the correct queue table"""
    table_name = "RPT.SMS_Q_TABLE" if msg_type.lower() == "sms" else "RPT.EMAIL_Q_TABLE"

    pool = get_pool()
    with pool.acquire() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT STATUS FROM {table_name} WHERE ID = :msg_id",
                msg_id=msg_id
            )
            row = cur.fetchone()
            return row[0] if row else None