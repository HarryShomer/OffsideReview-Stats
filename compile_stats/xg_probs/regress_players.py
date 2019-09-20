"""
DEPRECATED - Used for shooter xg model
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import shared

import sys
sys.path.append("..")
from machine_info import *


def exchange_cols(df):
    """
    When get df's of previous seasons from db I run into problems with lowercase
    """
    if not df.empty:
        df['Game_Id'] = df['game_id']
        df['p1_ID'] = df['p1_id']
        df['Date'] = df['date']
        df['Event'] = df['event']
        df['Period'] = df['period']
        df = df.drop(['game_id', 'p1_id', 'date', 'event', 'period'], axis=1)

    return df


def assign_shots(season, play, players, avgs):
    """
    Assign Shot for given play
    """
    # Assign to avgs and player totals (but only for regular season)
    if play['Game_Id'] < 30000 and play['Event'] in ['SHOT', 'MISS', 'GOAL'] and play['Period'] != 5:
        # It happens...I don't know
        try:
            p1_id = str(int(play["p1_ID"]))
        except ValueError:
            return

        pos = players[p1_id]["pos"]

        # Player numbers
        players[p1_id]["data"][str(season)]["xg"] += play["xg"]
        players[p1_id]["data"][str(season)]["goals"] += play["if_goal"]
        players[p1_id]["data"][str(season)]["fen"] += 1

        # Add to totals
        avgs[str(season)][pos]["xg"] += play["xg"]
        avgs[str(season)][pos]["goals"] += play["if_goal"]
        avgs[str(season)][pos]["fen"] += 1


def get_previous_data(season):
    """
    For a given set of games it gets the previous games from that season (if there are any) and the data from the 2
    previous seasons

    :param season: ex: 2017

    :return: DataFrame of info 
    """
    engine = create_engine('postgresql://{}:{}@{}:5432/nhl_data'.format(USERNAME, PASSWORD, HOST))

    prev_dfs = []
    for i in range(3):
        if season > 2006:
            pbp = shared.get_season_table("pbp" + str(season), engine)
            if not pbp.empty:
                prev_dfs.append(pbp[['game_id', 'p1_id', 'date', 'event', 'period', 'xg']])
            season -= 1

    df = pd.concat(prev_dfs)

    # Get some conflict with upper/lower case keys so just change some here
    df = exchange_cols(df)

    return df


def get_players():
    """
    Get dict of all players. Each player has their own dict of each season which is used to hold their stats

    :return: dict of players
    """
    players = shared.get_player_info()

    players_dict = {}
    for player_id in players.keys():
        data = {"2007": {"xg": 0, "fen": 0, "goals": 0}, "2008": {"xg": 0, "fen": 0, "goals": 0},
                "2009": {"xg": 0, "fen": 0, "goals": 0}, "2010": {"xg": 0, "fen": 0, "goals": 0},
                "2011": {"xg": 0, "fen": 0, "goals": 0}, "2012": {"xg": 0, "fen": 0, "goals": 0},
                "2013": {"xg": 0, "fen": 0, "goals": 0}, "2014": {"xg": 0, "fen": 0, "goals": 0},
                "2015": {"xg": 0, "fen": 0, "goals": 0}, "2016": {"xg": 0, "fen": 0, "goals": 0},
                "2017": {"xg": 0, "fen": 0, "goals": 0}
                }
        players_dict[player_id] = {"data": data, "pos": players[player_id]["pos"]}

    return players_dict


def get_multiplier(play, players, avgs):
    """
    Assign the shot to the given player. Before we do that, using the player's previous two seasons (and that year)
    we determined the the regressed fsh% and xg index. 

    We also need to get the number to regress to. Originally I would regress to the mean of that year but that would be
    including what happens in the future. So I regress to the average of the previous season and whatever stats 
    accumulated so far for that given season. The issue here is with 2007 so the average for 2006 is considered the 
    average stats for a single season using the total data set. After regressing I divide by the average (which I just
    regressed to) to normalize the stat (since the average change over 10 years). 

    :param play: the given play
    :param players: dict of players
    :param avgs: seasonal average

    :return: regressed xg_index, regressed Fsh%
    """
    if play['Event'] not in ["GOAL", "SHOT", "MISS"] or play['Period'] == 5:
        return 0, 0

    # Constants for regressing (All and Ev)
    xg_const = {"F": 280, "D": 2350}

    season = int(shared.get_season(play['Date']))
    try:
        pos = players[str(int(play['p1_ID']))]["pos"]
    except ValueError:
        return 1

    # Averages of totals for season using 2007-2016 (excluding lockout)...fudged xg a drop to make index = 1
    total_avgs = {"F": {"fen": 72400, "goals": 5630, "xg": 5630},
                  "D": {"fen": 29010, "goals": 1003, "xg": 1003}}

    # Get Averages to regress to (logic explained in function description)
    if season == 2007:
        xg_avg = (total_avgs[pos]["goals"] + avgs[str(season)][pos]["goals"]) / \
                 (total_avgs[pos]["xg"] + avgs[str(season)][pos]["xg"])
    else:
        xg_avg = (avgs[str(season - 1)][pos]["goals"] + avgs[str(season)][pos]["goals"]) / \
                 (avgs[str(season - 1)][pos]["xg"] + avgs[str(season)][pos]["xg"])

    # This season and two previous seasons
    goals, fen, xg = 0, 0, 0
    p1_id = str(int(play["p1_ID"]))
    for i in range(0, 3):
        if season - i > 2006:
            goals += players[p1_id]["data"][str(season - i)]["goals"]
            fen += players[p1_id]["data"][str(season - i)]["fen"]
            xg += players[p1_id]["data"][str(season - i)]["xg"]

    xg_index = goals / xg if xg != 0 else 0
    reg_xg_index = xg_index - ((xg_index - xg_avg) * (xg_const[pos] / (xg_const[pos] + fen)))
    reg_xg_index /= xg_avg   # Normalize for season

    assign_shots(season, play, players, avgs)

    return reg_xg_index


def get_player_regress(df):
    """
    Get regressed xg_index for each shot

    :param df: Full DataFrame

    :return: DataFrame with both pieces of info
    """
    print("Getting xG multipliers")

    # Subset to needed cols
    df_calc = df[['Game_Id', 'p1_ID', 'Date', 'Event', 'Period', 'season', "Seconds_Elapsed", 'xg']]

    # For Summing
    df_calc['if_fen'] = np.where(df_calc['Event'].isin(["GOAL", "SHOT", "MISS"]), 1, 0)
    df_calc['if_goal'] = np.where(df_calc['Event'] == "GOAL", 1, 0)

    # Player/Position Shit
    players = get_players()

    # Avg Dict
    season_avgs = {}
    # TODO: Change 20!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    for i in range(7, 20):
        season_avgs[str(2000 + i)] = {
            "D": {"xg": 0, "fen": 0, "goals": 0},
            "F": {"xg": 0, "fen": 0, "goals": 0}
        }

    # Get previous data
    prev_df = get_previous_data(int(shared.get_season(df.iloc[0]["Date"])))
    if not prev_df.empty:
        prev_df['if_goal'] = np.where(prev_df['Event'] == "GOAL", 1, 0)
        #[assign_shots(shared.get_season(play['Date']), play, players, season_avgs) for play in prev_df.to_dict("records")]
        prev_df.apply(lambda play: assign_shots(shared.get_season(play['Date']), play, players, season_avgs), axis=1)
        del prev_df

    # Get multipliers
    df_calc = df_calc.sort_values(by=["season", "Game_Id", "Period", "Seconds_Elapsed"])
    plays = df_calc.to_dict("records")
    df['reg_xg'] = [get_multiplier(play, players, season_avgs) for play in plays]

    return df

