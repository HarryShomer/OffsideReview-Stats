"""
Gets the goalies for both teams and their "true" adj_Fsv%. 
"""
import pandas as pd
import json
import datetime
import helpers

import sys
sys.path.append("..")
from machine_info import *


def get_goalie_data(date):
    """
    Get Goalie Stats for building marcels:
    1. Read in 2015, 2016 and 2017 data
    2. Get Current Season data from Server

    :param date: Current Date

    :return: DataFrame with goalie data
    """
    # Get Yesterdays date
    yesterday = datetime.datetime.strptime(date, '%Y-%m-%d') - datetime.timedelta(1)
    yesterday = '-'.join([str(yesterday.year), str(yesterday.month), str(yesterday.day)])

    prev_season = str(int(helpers.get_season(date)) - 3)
    # Want three season prior
    # I don't have 2015 data on the site...so if 3 years prior is 2015 I have that year stored in a Csv
    # So just go back to years and append 2015 data
    if prev_season in [2014, 2015]:
        df_1415 = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data/goalie_2014_2015.csv'), index_col=0)

    # Get for 3 and change seasons
    url = "http://{}/goalies/Query/?strength=Even&split_by=Season&team=&venue=Both&season_type=All&" \
          "date_filter_from={}-10-01&date_filter_to={}&adjustment=Non%20Score%20Adjusted&toi=0&search=" \
        .format(site, prev_season, yesterday)

    df = pd.DataFrame(json.loads(helpers.get_page(url))['data'])

    # If 2014/2015...add in
    if prev_season in [2014, 2015]:
        df = df.append(df_1415)

    # Group up and sort
    df = df[['player',  'season', 'games', 'toi_on', 'goals_a', 'fenwick_a', 'xg_a']]
    df_stats = df.groupby(['player', 'season'], as_index=False)['games', 'toi_on', 'goals_a', 'fenwick_a', 'xg_a'].sum()
    df_stats = df_stats.sort_values(['player', 'season'])

    return df_stats


def marcels_players(goalie, date, df):
    """
    Get Marcels for a goalie 

    :param goalie: some goalie
    :param date: Date of games
    :param df: Data we use to make marcels

    :return: marcel for goalie
    """
    # 0 = that year, 1 is year b4 ....
    marcel_weights = [.36, .29, .21, .14]
    reg_const = 2000
    reg_avg = 0  # Where to regress to

    # Use past 3 season to weight games played -> Just take weighted average
    gp_weights = [8, 4, 2, 0]

    season = int(helpers.get_season(date))

    weighted_goals_sum, weighted_fen_sum, weighted_xg_sum, weights_marcel_sum = 0, 0, 0, 0
    weighted_gp_sum, weights_gp_sum = 0, 0

    # Past 4 Seasons
    for i in range(0, 4):
        if season - i > 2006:
            # Subset from stats df
            df_goalie = df[(df['player'] == goalie) & (df['season'] == (season - i))]

            # Sanity Check
            if df_goalie.shape[0] > 1:
                print("Too many rows!!!!!!!")
                exit()

            # If he played that year
            if not df_goalie.empty:
                weighted_goals_sum += df_goalie.iloc[0]['goals_a'] * marcel_weights[i]
                weighted_fen_sum += df_goalie.iloc[0]['fenwick_a'] * marcel_weights[i]
                weighted_xg_sum += df_goalie.iloc[0]['xg_a'] * marcel_weights[i]
                weighted_gp_sum += df_goalie.iloc[0]['games'] * gp_weights[i]

                # -> To divide by at end...normalize everything
                weights_marcel_sum += marcel_weights[i]
                weights_gp_sum += gp_weights[i]

    # Normalize weighted sums
    weighted_xg_sum = weighted_xg_sum / weights_marcel_sum if weights_marcel_sum != 0 else 0
    weighted_goals_sum = weighted_goals_sum / weights_marcel_sum if weights_marcel_sum != 0 else 0
    weighted_fen_sum = weighted_fen_sum / weights_marcel_sum if weights_marcel_sum != 0 else 0

    # Get Regressed fsv%
    if weighted_fen_sum != 0:
        weighted_adj_fsv = ((1 - weighted_goals_sum / weighted_fen_sum) - (1 - weighted_xg_sum / weighted_fen_sum)) * 100
    else:
        weighted_adj_fsv = 0
    reg_adj_fsv = weighted_adj_fsv - ((weighted_adj_fsv - reg_avg) * (reg_const / (reg_const + weighted_fen_sum)))

    # Get weighted gp
    weighted_gp_sum = weighted_gp_sum / weights_gp_sum if weights_gp_sum != 0 else 0

    return {'fsv': reg_adj_fsv, 'gp': weighted_gp_sum}


def get_marcels(goalies, date, df):
    """
    Get marcels for each game
    """
    goalies_marcels = []
    for goalie in goalies:
            goalie_marcels = marcels_players(goalie, date, df)
            goalies_marcels.append({"goalie": goalie, "adj_fsv": goalie_marcels['fsv'], "gp": goalie_marcels['gp']})

    return goalies_marcels


def main():
    goalies = ['HENRIK LUNDQVIST', "CAM TALBOT", "PEKKA RINNE"]
    print(get_marcels(goalies, "2018-08-11"))


if __name__ == "__main__":
    main()