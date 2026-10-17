import os
import sys
import json
import logging
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp":"%(asctime)s", "level":"%(levelname)s", "module":"%(name)s", "message":"%(message)s"}',
)
logger = logging.getLogger("debezium_manager")

CONNECT_TIMEOUT = 3.05
READ_TIMEOUT = 15.0
DEFAULT_DEBEZIUM_URL = os.environ.get("DEBEZIUM_CONNECT_URL", "http://localhost:8083")


def build_resilient_session(
    total_retries: int = 5, backoff_factor: float = 1.0
) -> requests.Session:
    retry_strategy = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT", "DELETE"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy, pool_connections=10, pool_maxsize=10
    )
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "DebeziumManager/1.0",
        }
    )
    return session


def wait_for_debezium_ready(
    session: requests.Session,
    base_url: str = DEFAULT_DEBEZIUM_URL,
    max_wait_sec: int = 60,
) -> bool:
    import time

    start = time.time()
    logger.info(f"Checking connection to Debezium API at {base_url}...")
    while time.time() - start < max_wait_sec:
        try:
            resp = session.get(f"{base_url}/", timeout=(CONNECT_TIMEOUT, 5.0))
            if resp.status_code == 200:
                logger.info("Kafka Connect REST API is ready!")
                return True
        except requests.RequestException:
            pass
        time.sleep(2)
    logger.error(f"Timeout waiting for Debezium to be ready ({max_wait_sec}s)")
    return False


def deploy_connector(
    session: requests.Session, config_path: str, base_url: str = DEFAULT_DEBEZIUM_URL
) -> bool:
    file_path = Path(config_path)
    if not file_path.exists():
        logger.error(f"File config not found: {config_path}")
        return False

    with open(file_path, "r", encoding="utf-8") as f:
        config_data = json.load(f)

    connector_name = config_data.get("name")
    connector_config = config_data.get("config")

    if not connector_name or not connector_config:
        logger.error(f"File {config_path} do not match the format")
        return False

    endpoint = f"{base_url}/connectors/{connector_name}/config"
    try:
        response = session.put(
            endpoint, json=connector_config, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
        if response.status_code in (200, 201):
            logger.info(
                f"Connector [{connector_name}] has been deployed successfully! (HTTP {response.status_code})"
            )
            return True
        else:
            logger.error(
                f"Deploy connector [{connector_name}] failed: HTTP {response.status_code} - {response.text}"
            )
            return False
    except requests.RequestException as e:
        logger.error(
            f"Network error when calling Debezium API for [{connector_name}]: {e}"
        )
        return False


def get_all_connectors_status(
    session: requests.Session, base_url: str = DEFAULT_DEBEZIUM_URL
):
    try:
        resp = session.get(
            f"{base_url}/connectors", timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
        )
        resp.raise_for_status()
        connectors = resp.json()
        logger.info(f"Current connectors ({len(connectors)}): {connectors}")

        for name in connectors:
            status_resp = session.get(
                f"{base_url}/connectors/{name}/status",
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
            if status_resp.status_code == 200:
                status_data = status_resp.json()
                connector_state = status_data.get("connector", {}).get(
                    "state", "UNKNOWN"
                )
                tasks = status_data.get("tasks", [])
                logger.info(
                    f"Connector [{name}] Status: {connector_state} | Tasks: {len(tasks)}"
                )
                for t in tasks:
                    if t.get("state") == "FAILED":
                        logger.error(
                            f"Task {t.get('id')} of [{name}] FAILED: {t.get('trace')[:200]}"
                        )
    except requests.RequestException as e:
        logger.error(f"Failed to query connector status: {e}")


if __name__ == "__main__":
    configs_dir = Path(__file__).parent / "connector_configs"
    with build_resilient_session() as http_session:
        if not wait_for_debezium_ready(http_session):
            sys.exit(1)

        for cfg_file in configs_dir.glob("*.json"):
            deploy_connector(http_session, str(cfg_file))

        get_all_connectors_status(http_session)
