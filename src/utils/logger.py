import logging
import sys

logger = logging.getLogger("nasa-tempo-etl")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
handler.setFormatter(formatter)

if not logger.hasHandlers():
    logger.addHandler(handler)

