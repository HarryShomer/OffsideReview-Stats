import pandas as pd
import json
import math
import helpers
import datetime
import get_goalie_stats as ggs

import sys
sys.path.append("..")
from machine_info import *

pd.options.mode.chained_assignment = None  # default='warn'


def get_data_from_server(strength, date):
    """
    Get specified data from server
    
    :param strength: ....
    :param date: Date of given game we are predicting
    
    :return: DataFrame with data
    """
    # Get Yesterdays date
    yesterday = datetime.datetime.strptime(date, '%Y-%m-%d') - datetime.timedelta(1)
    yesterday = '-'.join([str(yesterday.year), str(yesterday.month), str(yesterday.day)])

    # Want season prior for prev_stats
    prev_season = str(int(helpers.get_season(date)) - 1)

    url = "http://{}/teams/Query/?strength={}&split_by=Game&team=&venue=Both&season_type=All&date_filter_from={}-10-01&" \
          "date_filter_to={}&adjustment=Score%20Adjusted&toi=0".format(site, strength, prev_season, yesterday)

    response = helpers.get_page(url)

    return pd.DataFrame(json.loads(response)['data'])


def get_all_sits_data(date):
    """
    Get and prepare the All Situations data
    """
    cols = ["Team", "Season", "Game.ID", "Date", "Opponent", "Venue", "TOI_all", "PENT_all", "PEND_all"]

    df = get_data_from_server("All%20Situations", date)
    df = df.sort_values(by=['season', 'game_id', 'team'])
    for team_col in ['team', "opponent", "home"]:
        df = helpers.fix_team(df, team_col)

    df = df.rename(index=str, columns={"team": "Team", "season": "Season", "game_id": "Game.ID", "date": "Date",
                                       "opponent": "Opponent", "home": "Venue", "toi":  "TOI_all", "pent": "PENT_all",
                                       "pend": "PEND_all"})
    return df[cols]


def get_even_data(date):
    """
    Get and prepare the even strength data
    """
    cols = ["Team", "Season", "Game.ID", "Date", "TOI_even", 'GF_even', 'GA_even', 'FF_even', 'FA_even', 'xGF_even',
            'xGA_even', 'CF_even', 'CA_even']

    df = get_data_from_server("Even", date)
    df = helpers.fix_team(df, "team")
    df = df.sort_values(by=['season', 'game_id', 'team'])

    df = df.rename(index=str, columns={"team": "Team", "season": "Season", "game_id": "Game.ID", "date": "Date",
                                       "toi": "TOI_even", "goals_f": "GF_even", "goals_a": "GA_even",
                                       "fenwick_f": "FF_even", "fenwick_a": "FA_even", "xg_f": 'xGF_even',
                                       'xg_a': 'xGA_even', 'corsi_f': 'CF_even', 'corsi_a': 'CA_even'})
    return df[cols]


def get_pp_data(date):
    """
    Get and prepare the power play data
    """
    cols = ["Team", "Season", "Game.ID", "Date", "TOI_pp", 'GF_pp', 'FF_pp', 'xGF_pp', 'CF_pp']

    df = get_data_from_server("PP", date)
    df = helpers.fix_team(df, "team")
    df = df.sort_values(by=['season', 'game_id', 'team'])

    df = df.rename(index=str, columns={"team": "Team", "season": "Season", "game_id": "Game.ID", "date": "Date",
                                       "toi": "TOI_pp", "goals_f": "GF_pp",  "fenwick_f": "FF_pp", "xg_f": 'xGF_pp',
                                       'corsi_f': 'CF_pp'})
    return df[cols]


def get_pk_data(date):
    """
    Get and prepare the penalty kill data
    """
    cols = ["Team", "Season", "Game.ID", "Date", "TOI_pk", 'GA_pk', 'FA_pk', 'xGA_pk', 'CA_pk']

    df = get_data_from_server("PK", date)
    df = helpers.fix_team(df, "team")
    df = df.sort_values(by=['season', 'game_id', 'team'])

    df = df.rename(index=str, columns={"team": "Team", "season": "Season", "game_id": "Game.ID", "date": "Date",
                                       "toi": "TOI_pk", "goals_a": "GA_pk", "fenwick_a": "FA_pk", "xg_a": 'xGA_pk',
                                       'corsi_a': 'CA_pk'})
    return df[cols]


def team_preprocessing(date):
    """
    Get All the Data
    """
    df_all = get_all_sits_data(date)
    df_even = get_even_data(date)
    df_pp = get_pp_data(date)
    df_pk = get_pk_data(date)

    # Merge them all into one DataFrame
    df2 = pd.merge(df_all, df_even, how="left",
                   left_on=["Team", "Season", "Game.ID", "Date"],
                   right_on=["Team", "Season", "Game.ID", "Date"],
                   suffixes=['', "_even"])
    df3 = pd.merge(df2, df_pp, how="left",
                   left_on=["Team", "Season", "Game.ID", "Date"],
                   right_on=["Team", "Season", "Game.ID", "Date"],
                   suffixes=['', "_pp"])
    df_merged = pd.merge(df3, df_pk, how="left",
                         left_on=["Team", "Season", "Game.ID", "Date"],
                         right_on=["Team", "Season", "Game.ID", "Date"],
                         suffixes=['', "_pk"])

    df_merged = df_merged.sort_values(by=['Season', 'Game.ID', 'Team'])

    df_merged['game_id'] = df_merged.apply(lambda x: str(x['Season']) + "0" + str(x['Game.ID']), axis=1)

    # Fix some player columns...I don't know
    fix_cols = ['TOI_all', 'TOI_even', "TOI_pp", "TOI_pk", "FF_even", "FA_even", "xGF_even", "xGA_even", "CF_even",
                "CA_even", "FF_pp", "xGF_pp", "CF_pp", "FA_pk", "xGA_pk", "CA_pk"]
    for col in fix_cols:
        df_merged[col] = df_merged[col].astype(float)

    return df_merged


def get_days_since_last(df, prev_df):
    """
    Get days since last game for each team 
    """
    # Convert to date for both dfs
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    df['Date'] = df['Date'].dt.date
    prev_df['Date'] = pd.to_datetime(prev_df['Date'], format='%Y-%m-%d')
    prev_df['Date'] = prev_df['Date'].dt.date

    home_days, away_days = [], []
    for row in df.to_dict("records"):
        home_col = "Venue"
        away_col = "Opponent" if row['Team'] == row['Venue'] else "Team"

        # Home
        prev_games = prev_df[(prev_df["Team"] == row[home_col]) & (prev_df['Date'] < row['Date']) & (prev_df['Season'] == row['Season'])]
        home_days.append(5 if prev_games.empty else (row['Date'] - prev_games.iloc[prev_games.shape[0] - 1]['Date']).days)

        # Away
        prev_games = prev_df[(prev_df["Team"] == row[away_col]) & (prev_df['Date'] < row['Date']) & (prev_df['Season'] == row['Season'])]
        away_days.append(5 if prev_games.empty else (row['Date'] - prev_games.iloc[prev_games.shape[0] - 1]['Date']).days)

    df['days_rest_home'] = home_days
    df['days_rest_away'] = away_days

    return df


def calc_stats(df):
    """
    Calculate stats for given sample
    """

    # All
    df['PENT60'] = df['PENT_all'] * 60 / df['TOI_all']
    df['PEND60'] = df['PEND_all'] * 60 / df['TOI_all']

    # Even
    df['FF60_even'] = df['FF_even'] * 60 / df['TOI_even']
    df['FA60_even'] = df['FA_even'] * 60 / df['TOI_even']
    df['xGF60/FF60_even'] = df['xGF_even'] / df['FF_even']
    df['xGA60/FA60_even'] = df['xGA_even'] / df['FA_even']
    df['GF60/xGF60_even'] = df['GF_even'] / df['xGF_even']

    # PP
    df['FF60_pp'] = df['FF_pp'] * 60 / df['TOI_pp']
    df['xGF60/FF60_pp'] = df['xGF_pp'] / df['FF_pp']
    df['GF60/xGF60_pp'] = df['GF_pp'] / df['xGF_pp']

    # PK
    df['FA60_pk'] = df['FA_pk'] * 60 / df['TOI_pk']
    df['xGA60/FA60_pk'] = df['GA_pk'] / df['FA_pk']

    return df


def get_prev_stats_row(game, df, sum_cols, stats_cols):
    """
    Get the Stats for this row
    """
    # Make it a little easier
    game["Team"], game["Opponent"], game['Season'] = game['home_team'], game['away_team'], helpers.get_season(game['date'])

    row_dict = {"Team": game["home_team"], "Season": helpers.get_season(game['date']), "Date": game["date"],
                "Opponent": game["away_team"], "Venue": game['home_team'], "game_id": game["game_id"]}

    for team_col in ['Team', 'Opponent']:
        if_less_than_25 = True

        prev_stats_df = df[(df["Team"] == game[team_col]) & (df['Season'] == game['Season']) & (df['Date'] < game['date'])]
        if not prev_stats_df.empty:
            if_less_than_25 = False if prev_stats_df.shape[0] > 24 else True

            # We go to -1 to get 0 (which necessitate us starting one under the number of games)
            prev_stats_df['game_weight'] = [math.e ** (-.05 * x) for x in range(prev_stats_df.shape[0]-1, -1, -1)]

            # Get Weighted Average for each number
            weight_sum = prev_stats_df["game_weight"].sum()
            for col in sum_cols:
                prev_stats_df[col] = prev_stats_df[col] * prev_stats_df["game_weight"] / weight_sum

            # Sum and Get Stats for that year
            df_same_sum = prev_stats_df[sum_cols].sum()
            df_same = calc_stats(df_same_sum)

        # Check if need last years numbers..if so add in
        if if_less_than_25:
            prev_season_df = df[(df["Team"] == game[team_col]) & (df['Season'] == game['Season'] - 1)]
            if not prev_season_df.empty:
                df_last_sum = prev_season_df[sum_cols].sum()
            else:
                # Just take the league average when we got nothing for last year
                df_last_sum = df[sum_cols].sum()
            # Get Stats for previous year
            df_last = calc_stats(df_last_sum)

        # Assign the stats
        # If Less than 25 add in by given weight
        for stat in stats_cols:
            gp = prev_stats_df.shape[0]
            prev_yr_weight = math.e ** (-.175 * gp)
            if gp > 24:
                row_dict["_".join([stat, team_col])] = df_same[stat]
            elif gp > 0:
                row_dict["_".join([stat, team_col])] = (df_same[stat] * (1 - prev_yr_weight)) + (df_last[stat] * prev_yr_weight)
            else:
                row_dict["_".join([stat, team_col])] = df_last[stat]

    return row_dict


def get_previous_stats(games, df_season):
    """
    Get the previous stats for each game
    
    :param: games: Includes a date and both teams
    :param: df_season: DataFrame with game info for all previous games that season
    
    :return Stats for each game
    """
    sum_cols = ['TOI_all', 'PENT_all', 'PEND_all', 'TOI_even', 'GF_even', 'GA_even', 'FF_even', 'FA_even', 'xGF_even',
                'xGA_even', 'CF_even', 'CA_even', 'TOI_pp', 'GF_pp', 'FF_pp', 'xGF_pp', 'CF_pp', 'TOI_pk', 'GA_pk',
                'FA_pk', 'xGA_pk', 'CA_pk']

    stats_cols = ['PENT60', 'PEND60', 'FF60_even', 'FA60_even', 'xGF60/FF60_even', 'xGA60/FA60_even', 'GF60/xGF60_even',
                  'FF60_pp', 'xGF60/FF60_pp', 'GF60/xGF60_pp', 'FA60_pk', 'xGA60/FA60_pk']

    game_stats = []
    for game in games:
        game_stats.append(get_prev_stats_row(game, df_season, sum_cols, stats_cols))

    return pd.DataFrame(game_stats)


def get_model_data(games, date, goalie_df):
    """
    Get The Data required for the model
    """
    all_cols = ['game_id', 'Season',
                'FA60_even_Opponent', 'FA60_even_Team', 'FA60_pk_Opponent', 'FA60_pk_Team',
                'FF60_even_Opponent', 'FF60_even_Team', 'FF60_pp_Opponent', 'FF60_pp_Team',
                'GF60/xGF60_even_Opponent', 'GF60/xGF60_even_Team', 'GF60/xGF60_pp_Opponent', 'GF60/xGF60_pp_Team',
                'PEND60_Opponent', 'PEND60_Team', 'PENT60_Opponent', 'PENT60_Team',
                'xGA60/FA60_even_Opponent', 'xGA60/FA60_even_Team', 'xGA60/FA60_pk_Opponent', 'xGA60/FA60_pk_Team',
                'xGF60/FF60_even_Opponent', 'xGF60/FF60_even_Team', 'xGF60/FF60_pp_Opponent', 'xGF60/FF60_pp_Team',
                'days_rest_home', 'days_rest_away', 'home_b2b',  'away_b2b', 'home_adj_fsv', 'away_adj_fsv',
                'if_playoff']

    prev_df = team_preprocessing(date)
    prev_df = prev_df.fillna(0)

    # Fill with team stats and goalie stuff
    df = get_previous_stats(games, prev_df)
    df = get_days_since_last(df, prev_df)
    df = ggs.add_goalie_data(df, goalie_df, date)

    # Just get weighted average for adj_fsv
    df['home_adj_fsv'] = df['Home_Starter_adj_fsv'] * .946 + df['Home_Backup_adj_fsv'] * .053
    df['away_adj_fsv'] = df['Away_Starter_adj_fsv'] * .946 + df['Away_Backup_adj_fsv'] * .053

    # Only keeps games from the home team perspective!!!!!!!!
    df = df[df['Team'] == df['Venue']]

    # Add if a playoff game
    df['if_playoff'] = df.apply(lambda x: 1 if int(str(x['game_id'])[-5:]) > 30000 else 0, axis=1)

    # Fill in any missing value with the column average
    df = df.fillna(df.mean())

    return df[all_cols]


def main():
    games = [{'game_id': 2018020006,
             'date': "2018-10-04",
             'home_team': "NYR",
             'away_team': "NSH",
             'roster': {}
             }]

    get_model_data(games, "2018-10-04", None)


if __name__ == "__main__":
    main()
