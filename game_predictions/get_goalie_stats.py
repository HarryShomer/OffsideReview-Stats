"""
Gets the goalies for both teams and their "true" adj_Fsv%. 
"""
import pandas as pd
import json
import datetime
import os
import helpers
import get_starting_roster as gsr

import sys
sys.path.append("..")
from machine_info import *


def is_b2b(df, date):
    """
    Checks if B2B for starting goalie
    rosters -> Home/Away -> D/F/G -> {'Backup', 'Starter'}

    :param df: Games info
    :param date: Date of games

    :return: Pandas with b2b info in 
    """
    prev_rosters = gsr.get_yesterdays_rosters(date)

    # For each game search through roster (not the best complexity...but who cares)
    # NOTE: This is written horribly
    b2bs = {'Home': [], 'Away': []}
    for row in df.to_dict("records"):
        game_b2bs = {'Home': 0, 'Away': 0}
        for game_id in prev_rosters.keys():
            for venue in ["Home", "Away"]:
                if row[venue + "_Starter"] == prev_rosters[game_id]['players'][venue]["G"]["Starter"]:
                    game_b2bs[venue] = 1
        # Add in
        b2bs["Home"].append(game_b2bs["Home"])
        b2bs['Away'].append(game_b2bs['Away'])

    df['home_b2b'], df['away_b2b'] = b2bs['Home'], b2bs['Away']

    return df


def add_goalie_data(df, goalie_df, date):
    """
    1. Add the adj_fsv for starters and backups for both teams
    2. Calls function to get if a b2b for both teams starters
    
    :param df: DataFrame for goalie data to merge into
    :param goalie_df: DataFrame that is being merged in
    :param date: Today's date I guess
    
    :return Merged DataFrame
    """
    # Get b2b's first
    goalie_df = is_b2b(goalie_df, date)

    df = pd.merge(df, goalie_df, how="left", on="game_id")

    # Drop unwanted bullshit
    return df.drop(['Away_Backup', 'Away_Starter', 'Home_Backup', 'Home_Starter'], axis=1)


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
    if int(prev_season) == 2015:
        df_2015 = pd.read_csv(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data/goalie_2015_data.csv'), index_col=0)

    # Get for 3 and change seasons
    url = "http://{}/goalies/Query/?strength=Even&split_by=Season&team=&venue=Both&season_type=All&" \
          "date_filter_from={}-10-01&date_filter_to={}&adjustment=Non%20Score%20Adjusted&toi=0&search="\
        .format(site, prev_season, yesterday)

    df = pd.DataFrame(json.loads(helpers.get_page(url))['data'])

    # If 2015...add in
    if int(helpers.get_season(date)) - 3 == 2015:
        df = df.append(df_2015)

    # Group up and sort
    df = df[['player', 'season', 'games', 'toi_on', 'goals_a', 'fenwick_a', 'xg_a']]
    df_stats = df.groupby(['player', 'season'], as_index=False)['games', 'toi_on', 'goals_a', 'fenwick_a', 'xg_a'].sum()
    df_stats = df_stats.sort_values(['player', 'season'])

    return df_stats


def marcels_players(game, date, df):
    """
    Get Marcels for each goalie in a game
    
    :param game: Dictionary that holds the Goalies in a game
    :param date: Date of games
    :param df: Data we use to make marcels
    
    :return: marcel for each goalie
    """
    # 0 = that year, 1 is year b4 ....
    marcel_weights = [.36, .29, .21, .14]
    reg_const = 2000
    reg_avg = 0  # Where to regress to
    season = int(helpers.get_season(date))

    goalie_marcels = {}
    for goalie in ['Home_Starter', 'Away_Starter', 'Home_Backup', 'Away_Backup']:
        weighted_goals_sum, weighted_fen_sum, weighted_xg_sum, weights_sum = 0, 0, 0, 0

        # Past 4 Seasons
        for i in range(0, 4):
            if season - i > 2006:
                # Subset from stats df
                df_goalie = df[(df['player'] == game[goalie]) & (df['season'] == (season - i))]

                # Sanity Check
                if df_goalie.shape[0] > 1:
                    print("Too many rows!!!!!!!")
                    exit()

                # If he played that year
                if not df_goalie.empty:
                    weighted_goals_sum += df_goalie.iloc[0]['goals_a'] * marcel_weights[i]
                    weighted_fen_sum += df_goalie.iloc[0]['fenwick_a'] * marcel_weights[i]
                    weighted_xg_sum += df_goalie.iloc[0]['xg_a'] * marcel_weights[i]

                    # -> To divide by at end...normalize everything
                    weights_sum += marcel_weights[i]

        # Normalize weighted sums
        weighted_xg_sum = weighted_xg_sum / weights_sum if weights_sum != 0 else 0
        weighted_goals_sum = weighted_goals_sum / weights_sum if weights_sum != 0 else 0
        weighted_fen_sum = weighted_fen_sum / weights_sum if weights_sum != 0 else 0

        # Get Regressed
        if weighted_fen_sum != 0:
            weighted_adj_fsv = ((1 - weighted_goals_sum / weighted_fen_sum) - (1 - weighted_xg_sum / weighted_fen_sum)) * 100
        else:
            weighted_adj_fsv = 0
        reg_adj_fsv = weighted_adj_fsv - ((weighted_adj_fsv - reg_avg) * (reg_const / (reg_const + weighted_fen_sum)))

        # Add to game dictionary for that goalie
        goalie_marcels[goalie + "_adj_fsv"] = reg_adj_fsv

    return goalie_marcels


def get_marcels(rosters, date):
    """
    Get marcels for each game
    """
    cols = ["game_id", "Home_Starter", "Home_Starter_adj_fsv", "Away_Starter", "Away_Starter_adj_fsv", "Home_Backup",
            "Home_Backup_adj_fsv", "Away_Backup", "Away_Backup_adj_fsv"]

    # Get Data used to build this shit
    df = get_goalie_data(date)

    game_goalies = []
    for game in rosters:
        # Only if 2 each!!!!!!!!
        if len(game['roster']['players']["Home"]["G"].keys()) == 2 and len(game['roster']['players']["Away"]["G"].keys()) == 2:
            # Add general info
            game_dict = {"game_id": game['game_id'],
                         "Home_Starter": game['roster']['players']["Home"]["G"]["Starter"],
                         "Away_Starter": game['roster']['players']["Away"]["G"]["Starter"],
                         "Home_Backup": game['roster']['players']["Home"]["G"]["Backup"],
                         "Away_Backup": game['roster']['players']["Away"]["G"]["Backup"]}

            # Get Marcels for game and update in
            goalie_marcels = marcels_players(game_dict, date, df)
            game_dict.update(goalie_marcels)

            game_goalies.append(game_dict)

    return pd.DataFrame(game_goalies, columns=cols)


def get_goalies(rosters, date):
    """
    Get goalies for both teams (indicate if starter) and their marcel adj_fsv%
    
    NOTE: Throws out games without two goalies for each side!!!!!!! (there are very few...)
    """
    return get_marcels(rosters, date)


def main():
    pass

if __name__ == "__main__":
    main()

