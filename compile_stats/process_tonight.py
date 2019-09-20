import datetime
import sys
import os
import logging
import inspect

# Change path to make shit easier
# Needs to be done b4 import compile!!!!!
prev_path = os.getcwd()
os.chdir(os.path.dirname(os.path.abspath(__file__)))
from compile_stats import compile


yesterday = datetime.date.today() - datetime.timedelta(1)
date = '-'.join([str(yesterday.year), str(yesterday.month), str(yesterday.day)])

logging.basicConfig(filename='/home/harry/logs/compile_stats_errors.log', format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')

try:
    scrape_errors = compile.process(date, date)

    # Raise exception if there were any errors so we can put in logs and print to standard error
    if len(scrape_errors) > 0:
        raise Exception(scrape_errors)
except Exception as e:
    # Change back
    os.chdir(prev_path)

    print("Games on {} failed to compile\n".format(date), file=sys.stderr)
    logging.error(" [error] file='{file}' message='{error}'".format(file=inspect.trace()[-1].filename, error=e))

