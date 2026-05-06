import clickhouse_connect
import src.common.global_variables as config


def get_clickhouse_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_HTTP_PORT,
        username=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
    )
