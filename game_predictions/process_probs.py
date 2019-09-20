import os
import hockey_scraper as hs
import pandas as pd
from sklearn.externals import joblib
from sqlalchemy import create_engine
import get_goalie_stats as ggs
import get_starting_roster as gsr
import helpers
from models import elo_ratings, player_model, team_model, update_elo

import sys
sys.path.append("..")
from machine_info import *


def merge_team_elo(elo_df, team_df):
    """
    Merge the home_prob from elo into team
    
    :param elo_df: DataFrame with elo-derived probabilities for each game
    :param team_df: DataFrame with team features
    
    :return: Team DataFrame with merged info
    """
    elo_df['elo_prob'] = elo_df['home_prob']

    return pd.merge(team_df, elo_df[['game_id', 'elo_prob']], on=['game_id'])


def get_schedule_and_roster(date):
    """
    Get Json schedule for a specific date and get the rosters for each game that day
    
    If the roster isn't there it doesn't add it in

    :param date: YYYY-MM-DD

    :return: json of schedule
    """
    hs.shared.docs_dir = scraper_data_dir
    schedule = hs.nhl.json_schedule.get_schedule(date, date)

    games = []
    for date in schedule['dates']:
        for game in date['games']:
            # Get Roster first - if not there don't add in this time around
            try:
                roster = gsr.get_roster(game['gamePk'])
            except Exception:
                roster = None
            if roster:
                games.append({'game_id': game['gamePk'],
                              'date': date['date'],
                              'home_team': helpers.TEAMS[game['teams']['home']['team']['name'].upper()],
                              'away_team': helpers.TEAMS[game['teams']['away']['team']['name'].upper()],
                              'roster': roster
                              })
            else:
                print("The roster for game {} is not there".format(game['gamePk']))

    return games


def get_team_probs(team_df):
    """
    Get probabilities for team model
    
    :param team_df: DataFrame of team model data
    
    :return: List of probs 
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
    non_scaled = ['elo_prob']
    dummies = ['home_b2b', 'away_b2b']

    # Scale only continuous vars
    scaler = joblib.load(os.path.join(os.path.dirname(os.path.realpath(__file__)), "clfs/team_scaler.pkl"))
    team_df[continuous_vars] = scaler.transform(team_df[continuous_vars])

    # Subset features for model
    features = team_df[continuous_vars + non_scaled + dummies].values.tolist()

    # Get Predictions
    clf = joblib.load(os.path.join(os.path.dirname(os.path.realpath(__file__)), "clfs/team_classifier.pkl"))
    probs = clf.predict_proba(features)

    # Get just Home Probs
    return [prob[1] for prob in probs]


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
    scaler = joblib.load(os.path.join(os.path.dirname(os.path.realpath(__file__)), "clfs/player_scaler.pkl"))
    player_df[continuous_vars] = scaler.transform(player_df[continuous_vars])

    # Subset features for model
    features = player_df[continuous_vars + dummies].values.tolist()

    # Get Predictions
    clf = joblib.load(os.path.join(os.path.dirname(os.path.realpath(__file__)), "clfs/player_classifier.pkl"))
    probs = clf.predict_proba(features)

    # Get just Home Probs
    return [prob[1] for prob in probs]


def get_meta_probs(elo_df, team_df, player_df):
    """
    Get probabilities for meta-classifier

    :param elo_df: Elo Data
    :param team_df: Team Data
    :param player_df: Player Data
    
    :return: Dictionary of game_id, home_team, away_team, and Probs!!!!!
    """
    ensemble_df = pd.DataFrame()

    # Add in all the info first
    ensemble_df['game_id'] = player_df['game_id']
    ensemble_df['date'] = player_df['date']
    ensemble_df['home_team'] = player_df['home_team']
    ensemble_df['away_team'] = player_df['away_team']

    # Merge elo into teams and move get probs and move into the ensemble
    team_df = merge_team_elo(elo_df, team_df)
    ensemble_df['team_probs'] = get_team_probs(team_df)
    ensemble_df['player_probs'] = get_player_probs(player_df)
    ensemble_df['elo_probs'] = team_df['elo_prob']

    # Get Predictions and add it in
    features = ensemble_df[['team_probs', 'player_probs']].values.tolist()
    clf = joblib.load(os.path.join(os.path.dirname(os.path.realpath(__file__)), "clfs/meta_classifier.pkl"))
    ensemble_df['meta_probs'] = [prob[1] for prob in clf.predict_proba(features)]

    return ensemble_df


def process(date):
    """
    1. Build all the models for a given date
    2. Push data to DB
    """
    print("Calculating the Game Predictions for {}\n".format(date))

    # If we don't have any rosters yet we just leave
    games = get_schedule_and_roster(date)

    if not games:
        print("\nNo games were processed yet.")
        return

    goalie_marcels = ggs.get_goalies(games, date)

    # Get the Probs
    elo = elo_ratings.get_elo(games)
    update_elo.update_team_elo(date)
    teams = team_model.get_model_data(games, date, goalie_marcels)
    players = player_model.get_model_data(games, date, goalie_marcels)
    probs_df = get_meta_probs(elo, teams, players)

    # Push to db
    #engine = create_engine('postgresql://{}:{}@{}:5432/{}'.format(USERNAME, PASSWORD, HOST, SITE_DB))
    #probs_df.to_sql('game_preds_gamepreds', engine, if_exists='append', index=False)

    #probs_df.to_csv("csvs/" + date + ".csv", sep=',', index=False)
    return probs_df


def main():
    dfs = []
    for df in os.listdir("./csvs"):
        dfs.append(pd.read_csv(f"./csvs/{df}"))

    x = pd.concat(dfs)[["game_id", "home_team", "away_team", "meta_probs"]]
    x = x.set_index("game_id")
    x = x.rename(index=str, columns={"meta_probs": "home_prob"})

    import json
    with open("game_probs.json", "w") as f:
        json.dump(x.to_dict("index"), f, indent=4)


if __name__ == "__main__":
    main()