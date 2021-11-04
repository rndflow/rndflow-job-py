import sys
import logging

def make_logger():
    log = logging.getLogger('rndflow-job')
    log.setLevel(logging.INFO)

    cnl = logging.StreamHandler(sys.stdout)
    cnl.setLevel(logging.INFO)

    fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    cnl.setFormatter(fmt)
    log.addHandler(cnl)

    return log

logger = make_logger()
