import logging
import sys
from .config import Settings

_cfg = Settings()

#---------------------------------------------------------------------------
def _make_stdout_logger(name='rndflow-job'):
    level = logging.getLevelName(_cfg.rndflow_logging_level)
    log = logging.getLogger(name)
    log.setLevel(level)
    cnl = logging.StreamHandler(sys.stdout)
    cnl.setLevel(level)
    fmt = logging.Formatter('[%(asctime)s] %(message)s', _cfg.rndflow_dateformat)
    cnl.setFormatter(fmt)
    log.addHandler(cnl)
    return log

logger = _make_stdout_logger()

#---------------------------------------------------------------------------

def make_file_stdout_logger(file, name='rndflow-job'):
    level = logging.getLevelName(Settings().rndflow_logging_level)

    log = logging.getLogger(name)
    log.setLevel(level)
    cnl = logging.StreamHandler(sys.stdout)
    cnl.setLevel(level)
    cnf = logging.FileHandler(file)
    cnf.setLevel(level)

    fmt = logging.Formatter('[%(asctime)s] %(message)s', _cfg.rndflow_dateformat)
    cnl.setFormatter(fmt)
    cnf.setFormatter(fmt)
    log.addHandler(cnl)
    log.addHandler(cnf)

    return log
