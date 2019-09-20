import datetime
import sys
import logging
import inspect
import process_probs

today = datetime.date.today()
date = '-'.join([str(today.year), str(today.month), str(today.day)])

logging.basicConfig(filename='/home/harry/logs/game_preds_errors.log', format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')

try:
    process_probs.process(date)
except Exception as e:
    print("Calculating Game Predictions on {} failed.\n".format(date), file=sys.stderr)
    logging.error(" [error] file='{file}' message='{error}'".format(file=inspect.trace()[-1].filename, error=e))
