import datetime
import sys
import logging
import inspect
import update_elo

today = datetime.date.today()
date = '-'.join([str(today.year), str(today.month), str(today.day)])

logging.basicConfig(filename='/home/harry/logs/elo_ratings_errors.log', format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')

try:
    update_elo.update_team_elo()
except Exception as e:
    print(f"Updating Elo Ratings on {date} failed.\n", file=sys.stderr)
    logging.error(" [error] file='{file}' message='{error}'".format(file=inspect.trace()[-1].filename, error=e))