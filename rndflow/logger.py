import sys
import logging

from .config import Settings

def make_logger():
    cfg = Settings()

    level = logging.getLevelName(cfg.rndflow_logging_level)

    log = logging.getLogger('rndflow-job')
    log.setLevel(level)

    cnl = logging.StreamHandler(sys.stdout)
    cnl.setLevel(level)

    fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', cfg.rndflow_dateformat)
    cnl.setFormatter(fmt)
    log.addHandler(cnl)

    return log

logger = make_logger()
