from typing import Optional, Union
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    rndflow_api_server: Optional[str] = None
    rndflow_refresh_token: str

    rndflow_ssl_verify: bool = True

    rndflow_common_conn_retry_total: int = 5
    rndflow_common_conn_retry_read: int = 5
    rndflow_common_conn_retry_connect: int = 5
    rndflow_common_conn_retry_redirect: int = 5
    rndflow_common_conn_retry_status: int = 5
    rndflow_common_conn_retry_other: int = 5
    rndflow_common_conn_retry_backoff_factor: float = 0.3

    # https://docs.python-requests.org/en/latest/user/advanced/#timeouts
    # https://docs.python-requests.org/en/latest/api/#requests.adapters.HTTPAdapter.send
    rndflow_common_conn_timeout: Union[int, float] = 300
    rndflow_common_conn_read_timeout: Union[int, float] = 300

    # https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks
    rndflow_spec_conn_retry_total: int = 10
    rndflow_spec_conn_retry_read: int = 0
    rndflow_spec_conn_retry_connect: int = 10
    rndflow_spec_conn_retry_redirect: int = 5
    rndflow_spec_conn_retry_status: int = 10
    rndflow_spec_conn_retry_other: int = 0
    rndflow_spec_conn_retry_backoff_factor: float = 0.1    # {backoff factor} * (2 ** ({number of total retries} - 1))

    rndflow_spec_conn_timeout: Union[int, float] = 300
    rndflow_spec_conn_read_timeout: Union[int, float] = 300000
