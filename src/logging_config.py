import logging
import os
import datetime
import sys
def setup_logging(script_name, level=logging.DEBUG):
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logtime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # log_file = os.path.join(log_dir, f'{script_name}{logtime}.log')
    log_file = os.path.join(log_dir, f'{script_name}.log')
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)  # Explicitly log to stdout
        ]
    )

    logger = logging.getLogger(script_name)
    return logger