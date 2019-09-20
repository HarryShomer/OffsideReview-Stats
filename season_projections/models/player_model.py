import datetime
import json
import pandas as pd
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
          "date_filter_from={}-10-01&date_filter_to={}&adjustment=Score%20Adjusted&position=&toi=0&stats_view=All" \
        .format(site, strength, prev_season, yesterday)

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
    df_stats = df_stats.sort_values(['player', 'player_id', 'season'])

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
    df_stats = df_stats.sort_values(['player', 'player_id', 'season'])

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

    df['player_id'] = df['player_id'].astype(int)

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


def get_marcels_player(player_id, df, pos, season):
    """
    Get the Marcels for a given player
    """
    weighted_stats = {'toi_on_all': 0, 'goals': 0, 'a1': 0, 'a2': 0, 'icors': 0, 'iblocks': 0, 'pend': 0, 'pent': 0,
                      'ifac_win': 0, 'ifac_loss': 0, 'toi_on_even': 0, 'corsi_f': 0, 'corsi_a': 0, 'goals_f': 0,
                      'goals_a': 0, 'toi_all_gp': 0, 'gs_sum': 0, 'toi_sum': 0, 'gp': 0}

    # Get Stats (and weight them) for the Past 3 Seasons
    for i in range(0, 3):
        if int(season) - i > 2006:
            # Subset from stats df for a player
            df_skater = df[(df["player_id"] == player_id) & (df['season'] == (season - i)) & (df['position'] == pos)]

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
    weighted_stats['toi_all_gp'] = weighted_stats['toi_all_gp'] / weighted_stats['toi_sum'] if weighted_stats[
                                                                                                   'toi_sum'] != 0 else 0
    weighted_stats['gp'] = weighted_stats['gp'] / weighted_stats['toi_sum'] if weighted_stats['toi_sum'] != 0 else 0
    toi_per_gp = weighted_stats['toi_all_gp'] / weighted_stats['gp'] if weighted_stats['gp'] != 0 else 0

    # Calculate regressed game score and toi for player
    weighted_gs = calc_game_score(weighted_stats) if weighted_stats['toi_on_all'] != 0 else 0
    reg_gs = weighted_gs - ((weighted_gs - reg_avgs[pos]["gs60"]) * (reg_consts[pos]["gs60"] /
                                                                           (reg_consts[pos]["gs60"] + weighted_stats['toi_on_all'])))
    reg_toi = toi_per_gp - ((toi_per_gp - reg_avgs[pos]["toi/gp"]) * (reg_consts[pos]["toi/gp"] /
                                                                      (reg_consts[pos]["toi/gp"] + weighted_stats['toi_all_gp'])))

    return {'gs': reg_gs, 'toi': reg_toi}


def get_marcels(roster, date, stats_df):
    """
    1. Go through the roster and calculate the marcels for each player given what we already have stored
    2. Then Filter all from stats df of that specific game_id and assign the stats
    """
    season = helpers.get_season(date)
    skaters_marcels = {'F': [], 'D': []}

    for pos in roster.keys():
        for player in roster[pos]:
            player_marcels = get_marcels_player(player, stats_df, pos, season)
            skaters_marcels[pos].append(player_marcels)

    return skaters_marcels


def main():
    pass


if __name__ == "__main__":
    main()