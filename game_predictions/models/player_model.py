import datetime
import json
import pandas as pd
import get_goalie_stats as ggs
import helpers
from models.skater_marcels_consts import *

import sys
sys.path.append("..")
from machine_info import *


def get_data_from_server(strength, date):
    """
    Get specified data from server

    :param strength: ...
    :param date: Date of given game we are predicting
    
    :return: DataFrame with data
    """
    # Get Yesterdays date
    yesterday = datetime.datetime.strptime(date, '%Y-%m-%d') - datetime.timedelta(1)
    yesterday = '-'.join([str(yesterday.year), str(yesterday.month), str(yesterday.day)])

    # Want 2 season prior for prev_stats
    prev_season = str(int(helpers.get_season(date)) - 2)

    url = "http://{}/skaters/Query/?strength={}&split_by=Season&team=&search=&venue=Both&season_type=All&" \
          "date_filter_from={}-10-01&date_filter_to={}&adjustment=Score%20Adjusted&position=&toi=0&stats_view=All"\
        .format(site, strength, prev_season,  yesterday)

    response = helpers.get_page(url)

    return pd.DataFrame(json.loads(response)['data'])


def get_even_data(date):
    """
    Process the even strength DataFrame
    
    param: date: Get data until given date
    
    :return: Processed Even Strength DataFrame
    """
    cols = ['player', 'player_id', 'season', 'toi_on', 'corsi_f', 'corsi_a', 'goals_f', 'goals_a']

    df = get_data_from_server("Even", date)
    # Convert from string to float for some reason
    for col in ["toi_on", "corsi_f", "corsi_a"]:
        df[col] = df[col].astype(float)

    # Group and sort
    df = df[cols]
    df_stats = df.groupby(['player', 'player_id', 'season'], as_index=False)['toi_on', 'corsi_f', 'corsi_a', 'goals_f', 'goals_a'].sum()
    df_stats = df_stats.sort_values(['player', 'season'])

    # Change over some names for merging
    df_stats = df_stats.rename(index=str, columns={"toi_on": "toi_on_even"})

    return df_stats


def get_all_sits_data(date):
    """
    Process the All Situations DataFrame
    
    param: date: Get data until given date

    :return: Processed All Situations DataFrame
    """
    cols = ['player', 'player_id', 'season', 'position',
            'toi_on_all', 'goals', 'a1', 'a2', 'icors', 'iblocks', 'pend', 'pent', 'ifac_win', 'ifac_loss', 'games']

    df = get_data_from_server("All%20Situations", date)

    # Idk
    df['toi_on'] = df['toi_on'].astype(float)

    # Change over some names for merging
    df = df.rename(index=str, columns={"toi_on": "toi_on_all"})

    # Group and sort
    df = df[cols]
    df_stats = df.groupby(['player', 'player_id', 'season', 'position'], as_index=False)[
        'toi_on_all', 'goals', 'a1', 'a2', 'icors', 'iblocks', 'pend', 'pent', 'ifac_win', 'ifac_loss', 'games'].sum()
    df_stats = df_stats.sort_values(['player', 'season'])

    return df_stats


def get_raw_data(date):
    """
    Get the raw data by position and strength
    
    :param date: Today's date
    
    :return: DataFrame of combined stats for even and all 
    """
    df_all = get_all_sits_data(date)
    df_even = get_even_data(date)

    df = pd.merge(df_all, df_even, how="left", on=['player', 'player_id', 'season'])
    df = df.sort_values(['season', 'player', 'player_id'])

    # Divide positions into D and F
    df['position'] = df.apply(lambda x: 'F' if x['position'] != 'D' else 'D', axis=1)

    return df


def calc_game_score(player):
    """
    Calculate game score per 60 for a player (it's the weighted sample)
    
    :param player: Some Asshole
    
    :return: weighted game score per 60 given by marcel weighting
    """
    # Calculate Game Score and Game Score per 60
    player['gs'] = (.75 * player['goals']) + (.7 * player['a1']) + (.55 * player['a2']) + (.049 * player['icors']) \
                   + (.05 * player['iblocks']) + (.15 * player['pend']) - (.15 * player['pent']) \
                   + (.01 * player['ifac_win']) - (.01 * player['ifac_loss']) + (.05 * player['corsi_f']) \
                   - (.05 * player['corsi_a']) + (.15 * player['goals_f']) - (.15 * player['goals_a'])

    return player['gs'] * 60 / player['toi_on_all']


def get_marcels_player(game, df, player_col, season):
    """
    Get the Marcels for a given player
    """
    pos = player_col[5]
    weighted_stats = {'toi_on_all': 0, 'goals': 0, 'a1': 0, 'a2': 0, 'icors': 0, 'iblocks': 0, 'pend': 0, 'pent': 0,
                      'ifac_win': 0, 'ifac_loss': 0, 'toi_on_even': 0, 'corsi_f': 0, 'corsi_a': 0, 'goals_f': 0,
                      'goals_a': 0, 'toi_all_gp': 0, 'gs_sum': 0, 'toi_sum': 0, 'gp': 0}

    # Get Stats (and weight them) for the Past 3 Seasons
    for i in range(0, 3):
        if int(season) - i > 2006:
            # Subset from stats df for a player
            # Position takes care of sebastian aho
            df_skater = df[(df["player"] == game[player_col]) & (df['season'] == (int(season) - i))
                           & (df['position'] == pos)]

            # Add all stats in by weight if empty
            if not df_skater.empty:
                weighted_stats['toi_on_all'] += df_skater.iloc[0]["toi_on_all"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['goals'] += df_skater.iloc[0]["goals"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['a1'] += df_skater.iloc[0]["a1"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['a2'] += df_skater.iloc[0]["a2"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['icors'] += df_skater.iloc[0]["icors"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['iblocks'] += df_skater.iloc[0]["iblocks"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['pend'] += df_skater.iloc[0]["pend"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['pent'] += df_skater.iloc[0]["pent"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['ifac_win'] += df_skater.iloc[0]["ifac_win"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['ifac_loss'] += df_skater.iloc[0]["ifac_loss"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['corsi_f'] += df_skater.iloc[0]["corsi_f"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['corsi_a'] += df_skater.iloc[0]["corsi_a"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['goals_f'] += df_skater.iloc[0]["goals_f"] * marcel_weights[pos]['gs60'][i]
                weighted_stats['goals_a'] += df_skater.iloc[0]["goals_a"] * marcel_weights[pos]['gs60'][i]

                weighted_stats['toi_all_gp'] += df_skater.iloc[0]["toi_on_all"] * marcel_weights[pos]['toi/gp'][i]
                weighted_stats['gp'] += df_skater.iloc[0]["games"] * marcel_weights[pos]['toi/gp'][i]

                # -> To divide by at end...normalize everything
                weighted_stats['gs_sum'] += marcel_weights[pos]['gs60'][i]
                weighted_stats['toi_sum'] += marcel_weights[pos]['toi/gp'][i]

    # Normalize for Game Score
    norm_cols = ['toi_on_all', 'goals', 'a1', 'a2', 'icors', 'iblocks', 'pend', 'pent', 'ifac_win', 'ifac_loss',
                 'corsi_f', 'corsi_a', 'goals_f', 'goals_a']
    for key in norm_cols:
        weighted_stats[key] = weighted_stats[key] / weighted_stats['gs_sum'] if weighted_stats['gs_sum'] != 0 else 0

    # Normalize for toi/gp
    weighted_stats['toi_all_gp'] = weighted_stats['toi_all_gp'] / weighted_stats['toi_sum'] if weighted_stats['toi_sum'] != 0 else 0
    weighted_stats['gp'] = weighted_stats['gp'] / weighted_stats['toi_sum'] if weighted_stats['toi_sum'] != 0 else 0
    toi_per_gp = weighted_stats['toi_all_gp'] / weighted_stats['gp'] if weighted_stats['gp'] != 0 else 0

    # Calculate regressed Game Score and toi for player
    weighted_wpaa = calc_game_score(weighted_stats) if weighted_stats['toi_on_all'] != 0 else 0
    reg_wpaa = weighted_wpaa - ((weighted_wpaa - reg_avgs[pos]["gs60"]) * (reg_consts[pos]["gs60"] /
                                                                           (reg_consts[pos]["gs60"] + weighted_stats['toi_on_all'])))
    reg_toi = toi_per_gp - ((toi_per_gp - reg_avgs[pos]["toi/gp"]) * (reg_consts[pos]["toi/gp"] /
                                                                      (reg_consts[pos]["toi/gp"] + weighted_stats['toi_all_gp'])))

    return {'gs': reg_wpaa, 'toi': reg_toi}


def get_marcels_game(game, stats_df):
    """
    1. Go through the roster for that game and calculate the marcels for each player given what we already have stored
    2. Then Filter all from stats df of that specific game_id and assign the stats
    """
    player_cols = ['Away_D_1', 'Away_D_2', 'Away_D_3', 'Away_D_4', 'Away_D_5', 'Away_D_6', 'Away_D_7', 'Away_D_8',
                   'Away_F_1', 'Away_F_10', 'Away_F_11', 'Away_F_12', 'Away_F_13', 'Away_F_14', 'Away_F_2', 'Away_F_3',
                   'Away_F_4', 'Away_F_5', 'Away_F_6', 'Away_F_7', 'Away_F_8', 'Away_F_9', 'Home_D_1', 'Home_D_2',
                   'Home_D_3', 'Home_D_4', 'Home_D_5', 'Home_D_6', 'Home_D_7', 'Home_D_8', 'Home_F_1', 'Home_F_10',
                   'Home_F_11', 'Home_F_12', 'Home_F_13', 'Home_F_14', 'Home_F_2', 'Home_F_3', 'Home_F_4', 'Home_F_5',
                   'Home_F_6', 'Home_F_7', 'Home_F_8', 'Home_F_9']

    season = str(game['game_id'])[:4]
    game_dict = {'Home': {'F': [], 'D': []}, 'Away': {'F': [], 'D': []}}

    for player_col in player_cols:
        # If it's empty or it doesn't even exist just skip over this pass (float means it's nan)
        if player_col in list(game.keys()) and type(game[player_col]) == str:
            player_marcels = get_marcels_player(game, stats_df, player_col, season)
        else:
            continue

        # Add to game dictionary for that skater
        game_dict['Home' if player_col[:4] == "Home" else 'Away'][player_col[5]].append(player_marcels)

    return game_dict


def get_marcels(rosters, df):
    """
    Convert Roster to DataFrame (don't include Goalies...will be added in later)
    rosters -> Home/Away -> players -> D/F/G -> {'Backup', 'Starter'}
    """
    game_skaters_list = []
    for game in rosters:
        skaters = {"game_id": game['game_id'], "date": game['date'],
                   "home_team": game['home_team'], 'away_team': game['away_team']}

        # Add Skaters in
        for venue in ['Home', 'Away']:
            for pos in ['F', 'D']:
                for player_index in range(len(game['roster']['players'][venue][pos])):
                        skaters['_'.join([venue, pos, str(player_index + 1)])] = game['roster']['players'][venue][pos][player_index]['player']

        # Get Marcels for game and update in
        skater_marcels = get_marcels_game(skaters, df)
        skater_marcels.update({"game_id": game['game_id'], "date": game['date'], "home_team": game['home_team'], 'away_team': game['away_team']})
        game_skaters_list.append(skater_marcels)

    return game_skaters_list


def convert_marcels_to_df(marcels_games):
    """
    Convert the list of games of marcels to a DataFrame.

    For some games, teams will differ from the usual 12 F and 6 D. In these cases any extra forwards are added to 
    the backend of the defensemen and any extra defensemen the same for the forward group.

    Ex: When - 13 F and 5 D, the 13th forward will be put in the 6th defensemen slot

    Possibilities (for home & away):
    1. 14 F & 4 D
    2. 13 F & 5 D
    3. 12 F & 6 D
    4. 11 F & 7 D
    5. 10 F & 8 D
    """
    from operator import itemgetter

    games_list = []
    for game in marcels_games:
        game_dict = {'game_id': game['game_id'], 'date': game['date'], 'home_team': game['home_team'],
                     'away_team': game['away_team']}
        for venue in ['Home', 'Away']:
            # Sort so that the player with the most projected toi is gets popped first for each position
            sorted_forwards = sorted(game[venue]["F"], key=itemgetter('toi'), reverse=False)
            sorted_defensemen = sorted(game[venue]["D"], key=itemgetter('toi'), reverse=False)

            # Assign forwards in order
            f_index = 1
            while sorted_forwards:
                game_dict['_'.join([venue, "F", str(f_index)])] = sorted_forwards.pop()['gs']
                f_index += 1

                # Can't have more than 12 Forwards
                if f_index > 12:
                    break

            # Assign defensemen in order
            d_index = 1
            while sorted_defensemen:
                game_dict['_'.join([venue, "D", str(d_index)])] = sorted_defensemen.pop()['gs']
                d_index += 1

                # Can't have more than 6 Defensemen
                if d_index > 6:
                    break

            # We check if we have any players for each position still available
            # If we do we assign them the other position continuing with the previous index
            # Ex: If still 1 Defensemen left they get put in as the 13th Forward
            # Note: This only runs when they are 18 skaters specified. I don't bother if there are extra. So if we
            # already have 6 defensemen I just break from that loop
            while sorted_forwards:
                if d_index > 6:
                    break
                game_dict['_'.join([venue, "D", str(d_index)])] = sorted_forwards.pop()['gs']
                d_index += 1
            while sorted_defensemen:
                if f_index > 12:
                    break
                game_dict['_'.join([venue, "F", str(f_index)])] = sorted_defensemen.pop()['gs']
                f_index += 1

        games_list.append(game_dict)

    return pd.DataFrame(games_list)


def get_model_data(games, date, goalie_df):
    """
    Get the data required for the model

    NOTE: This is missing b2b and days of rest...the roster_df is used to construct the DataFrame and it doesn't include
    the Date. I could just merge it then calculate them but it's easier to just merge the player and team df's for the
    appropriate data. 
    """
    # Get the data for past 2 and change
    df_stats = get_raw_data(date)

    # Get Marcels for each player
    game_marcels = get_marcels(games, df_stats)

    # Convert marcels to a DataFrame
    df = convert_marcels_to_df(game_marcels)

    # Add Goalie Data for starter and backup
    df = ggs.add_goalie_data(df, goalie_df, date)

    # Fill in any missing value with the column average
    df = df.fillna(df.mean())

    return df


def main():
    pass


if __name__ == "__main__":
    main()
