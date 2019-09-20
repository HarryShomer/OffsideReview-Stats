import pandas as pd
import os
import random
from sklearn.externals import joblib

# This just makes it easier
player_scaler = joblib.load(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../clfs/player_scaler.pkl"))
team_scaler = joblib.load(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../clfs/team_scaler.pkl"))
player_clf = joblib.load(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../clfs/player_classifier.pkl"))
team_clf = joblib.load(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../clfs/team_classifier.pkl"))


def scale_team_feats(team_df):
    """
    Scale the team features
    
    NOTE: 'elo_prob' is not scaled and is not included at this point
    
    :param team_df: Team Model DataFrame
    
    :return: Scaled features DataFrame
    """
    continuous_vars = ['FA60_even_Opponent', 'FA60_even_Team',
                       'FA60_pk_Opponent', 'FA60_pk_Team',
                       'FF60_even_Opponent', 'FF60_even_Team',
                       'FF60_pp_Opponent', 'FF60_pp_Team',
                       'GF60/xGF60_even_Opponent', 'GF60/xGF60_even_Team',
                       'GF60/xGF60_pp_Opponent', 'GF60/xGF60_pp_Team',
                       'PEND60_Opponent', 'PEND60_Team',
                       'PENT60_Opponent', 'PENT60_Team',
                       'xGA60/FA60_even_Opponent', 'xGA60/FA60_even_Team',
                       'xGA60/FA60_pk_Opponent', 'xGA60/FA60_pk_Team',
                       'xGF60/FF60_even_Opponent', 'xGF60/FF60_even_Team',
                       'xGF60/FF60_pp_Opponent', 'xGF60/FF60_pp_Team',
                       'days_rest_home', 'days_rest_away',
                       'home_adj_fsv', 'away_adj_fsv']
    #non_scaled = ['elo_prob']
    dummies = ['home_b2b', 'away_b2b']

    # Switch it over -> Don't want to overwrite anything
    df_scaled = team_df[continuous_vars + dummies]

    # Scale only continuous vars
    df_scaled[continuous_vars] = team_scaler.transform(df_scaled[continuous_vars])

    return df_scaled[continuous_vars + dummies]


def get_team_probs(game):
    """
    Get probabilities for team model

    :param game: dictionary of game data -> includes model data

    :return: List of probs 
    """
    all_cols = ['FA60_even_Opponent', 'FA60_even_Team',
                'FA60_pk_Opponent', 'FA60_pk_Team',
                'FF60_even_Opponent', 'FF60_even_Team',
                'FF60_pp_Opponent', 'FF60_pp_Team',
                'GF60/xGF60_even_Opponent', 'GF60/xGF60_even_Team',
                'GF60/xGF60_pp_Opponent', 'GF60/xGF60_pp_Team',
                'PEND60_Opponent', 'PEND60_Team',
                'PENT60_Opponent', 'PENT60_Team',
                'xGA60/FA60_even_Opponent', 'xGA60/FA60_even_Team',
                'xGA60/FA60_pk_Opponent', 'xGA60/FA60_pk_Team',
                'xGF60/FF60_even_Opponent', 'xGF60/FF60_even_Team',
                'xGF60/FF60_pp_Opponent', 'xGF60/FF60_pp_Team',
                'days_rest_home', 'days_rest_away',
                'home_adj_fsv', 'away_adj_fsv', 'elo_prob',
                'home_b2b', 'away_b2b']

    features = [[game[col] for col in all_cols]]
    probs = team_clf.predict_proba(features)

    # Get just Home Probs
    return probs[0][1]


def get_player_probs(player_df):
    """
    Get probabilities for player model

    :param player_df: DataFrame of player model data

    :return: List of probs 
    """
    continuous_vars = ['Away_D_1', 'Away_D_2', 'Away_D_3', 'Away_D_4', 'Away_D_5', 'Away_D_6',
                       'Away_F_1', 'Away_F_2', 'Away_F_3', 'Away_F_4', 'Away_F_5', 'Away_F_6', 'Away_F_7', 'Away_F_8',
                       'Away_F_9', 'Away_F_10', 'Away_F_11', 'Away_F_12',
                       'Home_D_1', 'Home_D_2', 'Home_D_3', 'Home_D_4', 'Home_D_5', 'Home_D_6',
                       'Home_F_1', 'Home_F_2', 'Home_F_3', 'Home_F_4', 'Home_F_5', 'Home_F_6', 'Home_F_7', 'Home_F_8',
                       'Home_F_9', 'Home_F_10', 'Home_F_11', 'Home_F_12',
                       'Away_Backup_adj_fsv', 'Away_Starter_adj_fsv', 'Home_Backup_adj_fsv', 'Home_Starter_adj_fsv',
                       ]
    dummies = ['home_b2b', 'away_b2b']

    # Scale only continuous vars
    #start = time()
    player_df[continuous_vars] = player_scaler.transform(player_df[continuous_vars])
    #print("///////", time() - start)

    # Subset features for model
    features = player_df[continuous_vars + dummies].values.tolist()

    # Get Predictions
    probs = player_clf.predict_proba(features)

    # Get just Home Probs
    return [prob[1] for prob in probs]


def is_b2b(df, row):
    """
    Check if b2b for starting goalie

    Strategy: Get all previous games for that team first. Then check if they played yesterday and if the starting goalie
    was a starter yesterday (NOTE: We always check both teams when looking back)

    :param df:
    :param row: 

    :return: 
    """
    home_b2b, away_b2b = 0, 0

    # Home
    prev_games = df[((df["home_team"] == row['home_team']) | (df["away_team"] == row['home_team'])) & (df['date'] < row['date'])]
    if not prev_games.empty and (row['date'] - prev_games.iloc[prev_games.shape[0] - 1]['date']).days == 1 and \
                    row['Home_Starter'] in [prev_games.iloc[prev_games.shape[0] - 1]['Home_Starter'],
                                            prev_games.iloc[prev_games.shape[0] - 1]['Away_Starter']]:
        home_b2b = 1

    # Away
    prev_games = df[((df["home_team"] == row['away_team']) | (df["away_team"] == row['away_team'])) & (df['date'] < row['date'])]
    if not prev_games.empty and (row['Date'] - prev_games.iloc[prev_games.shape[0] - 1]['Date']).days == 1 and \
                    row['Away_Starter'] in [prev_games.iloc[prev_games.shape[0] - 1]['Home_Starter'],
                                            prev_games.iloc[prev_games.shape[0] - 1]['Away_Starter']]:
        away_b2b = 1

    return home_b2b, away_b2b


def choose_starter(df):
    """
    Randomly choose starting goalie for the game:
    In the Regular Season assume the "Starter" starts 70% of games
    In the Playoffs assume the "Starter" start 90% of games.

    If the backup is chosen we swap with the starter. 

    :param df: DataFrame of all games and players 

    :return: DataFrame with flipped goalies
    """
    new_rows = []

    for row in df.to_dict("records"):
        for venue in ["Home", "Away"]:
            # Makes the rest easier to read. The lines get too long
            starter_col, starter_fsv_col = "_".join([venue, 'Starter']), "_".join([venue, 'Starter_adj_fsv'])
            backup_col, backup_fsv_col = "_".join([venue, 'Backup']), "_".join([venue, 'Backup_adj_fsv'])

            # Regular Season = 70%
            if int(str(row['game_id'])[5:]) < 30000:
                # Means backup was chosen...then we flip it
                if random.randint(1, 10) > 7:
                    starter, starter_fsv = row[starter_col], row[starter_fsv_col]
                    row[starter_col], row[starter_fsv_col] = row[backup_col], row[backup_fsv_col]
                    row[backup_col], row[backup_fsv_col] = starter, starter_fsv
            # Playoffs = 90
            else:
                if random.randint(1, 10) > 9:
                    starter, starter_fsv = row[starter_col], row[starter_fsv_col]
                    row[starter_col], row[starter_fsv_col] = row[backup_col], row[backup_fsv_col]
                    row[backup_col], row[backup_fsv_col] = starter, starter_fsv

        new_rows.append(row)

    return pd.DataFrame(new_rows)


def get_last_game(row, df):
    """
    Get the last game for a team **THAT** season

    NOTE: If it's the first game of the season I just put 5
    """
    # Home
    prev_games = df[((df["home_team"] == row['home_team']) | (df["away_team"] == row['home_team'])) & (df['date'] < row['date'])]
    home_rest = 5 if prev_games.empty else (row['date'] - prev_games.iloc[prev_games.shape[0] - 1]['date']).days

    # Away
    prev_games = df[((df["home_team"] == row['away_team']) | (df["away_team"] == row['away_team'])) & (df['date'] < row['date'])]
    away_rest = 5 if prev_games.empty else (row['date'] - prev_games.iloc[prev_games.shape[0] - 1]['date']).days

    return home_rest, away_rest


def merge_players(df_schedule, df_players):
    """
    Merge the schedule and players DataFrames

    :param df_schedule: DataFrame of schedule info
    :param df_players: DataFrame of players info

    :return: merged DataFrame
    """
    stat_cols = ['Backup', 'Backup_adj_fsv', 'D_1', 'D_2', 'D_3', 'D_4', 'D_5', 'D_6', 'F_1', 'F_10', 'F_11', 'F_12',
                 'F_2', 'F_3', 'F_4', 'F_5', 'F_6', 'F_7', 'F_8', 'F_9', 'Starter', 'Starter_adj_fsv']

    # Rename columns for merge
    df_players_home = df_players.rename(index=str, columns={col: "Home_" + col for col in stat_cols})
    df_players_away = df_players.rename(index=str, columns={col: "Away_" + col for col in stat_cols})

    # Merge home then merge in away and drop extraneous columns
    df_players_merged = pd.merge(df_schedule, df_players_home, how="left", left_on=['home_team'], right_on=['team'])
    df_players_merged = pd.merge(df_players_merged, df_players_away, how="left", left_on=['away_team'], right_on=['team'])
    df_players_merged = df_players_merged.drop(['team_x', 'team_y'], axis=1)

    return df_players_merged


def merge_teams(df_schedule, df_teams):
    """
    Merge the schedule and teams DataFrames

    :param df_schedule: DataFrame of schedule info
    :param df_teams: DataFrame of team stats

    :return: merged DataFrame
    """
    stats_cols = ['PENT60', 'PEND60', 'FF60_even', 'FA60_even', 'xGF60/FF60_even', 'xGA60/FA60_even', 'GF60/xGF60_even',
                  'FF60_pp', 'xGF60/FF60_pp', 'GF60/xGF60_pp', 'FA60_pk', 'xGA60/FA60_pk']

    # Rename columns for merge
    df_teams_home = df_teams.rename(index=str, columns={col: col + "_Team" for col in stats_cols})
    df_teams_away = df_teams.rename(index=str, columns={col: col + "_Opponent" for col in stats_cols})

    # Merge home then merge in away and drop extraneous columns
    df_teams_merged = pd.merge(df_schedule, df_teams_home, how="left", left_on=['home_team'], right_on=['team'])
    df_teams_merged = pd.merge(df_teams_merged, df_teams_away, how="left", left_on=['away_team'], right_on=['team'])
    df_teams_merged = df_teams_merged.drop(['team_x', 'team_y'], axis=1)

    return df_teams_merged
