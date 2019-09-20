import datetime
import sys
import logging
import inspect
import os

# Change path to make shit easier
# Needs to be done b4 import stuff with 'machine_info.py'!!!!!
prev_path = os.getcwd()
os.chdir(os.path.dirname(os.path.abspath(__file__)))
import team_projections

today = datetime.date.today()
date = '-'.join([str(today.year), str(today.month), str(today.day)])

logging.basicConfig(filename='/home/harry/logs/season_projs_errors.log', format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')

try:
    team_projections.get_probs(date)
except Exception as e:
    os.chdir(prev_path)  # Change Back
    print("Calculating Game Predictions on {} failed.\n".format(date), file=sys.stderr)
    logging.error(" [error] file='{file}' message='{error}'".format(file=inspect.trace()[-1].filename, error=e))
