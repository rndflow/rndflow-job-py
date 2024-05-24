from typing import Optional, Union
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    api_server: Optional[str] = None
    refresh_token: str

    ssl_verify: bool = True

    common_conn_retry_total: int = 5
    common_conn_retry_read: int = 5
    common_conn_retry_connect: int = 5
    common_conn_retry_redirect: int = 5
    common_conn_retry_status: int = 5
    common_conn_retry_other: int = 5
    common_conn_retry_backoff_factor: float = 0.3

    # https://docs.python-requests.org/en/latest/user/advanced/#timeouts
    # https://docs.python-requests.org/en/latest/api/#requests.adapters.HTTPAdapter.send
    common_conn_timeout: Union[int, float] = 300
    common_conn_read_timeout: Union[int, float] = 300

    # https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks
    spec_conn_retry_total: int = 10
    spec_conn_retry_read: int = 0
    spec_conn_retry_connect: int = 10
    spec_conn_retry_redirect: int = 5
    spec_conn_retry_status: int = 10
    spec_conn_retry_other: int = 0
    spec_conn_retry_backoff_factor: float = 0.1    # {backoff factor} * (2 ** ({number of total retries} - 1))

    spec_conn_timeout: Union[int, float] = 300
    spec_conn_read_timeout: Union[int, float] = 300000

    logging_level: str = 'INFO'

    tz: str = Field('Europe/Moscow', alias='TZ')  # TZ env set by executor . Ignore common setting prefix by alias

    dateformat: str = '%d/%m/%Y %H:%M:%S %Z'

    model_config = SettingsConfigDict(env_prefix='rndflow_')

#print(str(Settings()).replace(' ', '\n'))
