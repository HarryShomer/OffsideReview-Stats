import hockey_scraper as hs
import pandas as pd
from sqlalchemy import create_engine
import helpers
import run_simulations
from models import team_model, get_model_probs as gmp, get_team_rosters as gtr

# Stop some Pandas error
# Needed!!! -> Because Output gets picked up by stderror
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

import sys
sys.path.append("..")
from machine_info import *


def get_year_schedule(date):
    """
    Get the schedule for that season
    
    :return: DataFrame
    """
    season = helpers.get_season(date)

    to_date = "-".join([str(season + 1), "07", "01"])

    hs.shared.docs_dir = scraper_data_dir
    schedule = hs.json_schedule.get_schedule(date, to_date)

    rows = []
    for date_games in schedule['dates']:
        for game in date_games['games']:
            # Only for non-all star and preseason games
            if 40000 > int(str(game['gamePk'])[5:]) >= 20000:
                rows.append({
                    "game_id": game['gamePk'],
                    "date": date_games['date'],
                    "home_team": helpers.TEAMS[game['teams']['home']['team']['name'].upper()],
                    "away_team": helpers.TEAMS[game['teams']['away']['team']['name'].upper()]
                })
    df = pd.DataFrame(rows)

    # Actual Datetime object
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df['Date'] = df['date'].dt.date

    return df


def get_team_player_info(date):
    """
    
    :return: 
    """
    # Team model data
    df_teams = team_model.get_model_data(date)

    # Skaters and Goalies Marcels
    df_players = gtr.get_rosters(date)

    # Schedule for all games from today onwards
    df_schedule = get_year_schedule(date)

    # Days of rest for each team
    df_schedule['days_rest_home'], df_schedule['days_rest_away'] = zip(*[gmp.get_last_game(row, df_schedule)
                                                                         for row in df_schedule.to_dict("records")])
    # Merge both
    df_teams_merged = gmp.merge_teams(df_schedule, df_teams)
    df_players_merged = gmp.merge_players(df_schedule, df_players)

    # Randomly choose starter for each game and then use that to check for b2b's
    df_players_merged = gmp.choose_starter(df_players_merged)
    df_players_merged['home_b2b'], df_players_merged['away_b2b'] = zip(*[gmp.is_b2b(df_players_merged, row)
                                                                       for row in df_players_merged.to_dict("records")])

    # Add Weighted adj_fsv over to teams and b2b's
    df_teams_merged['home_adj_fsv'] = df_players_merged['Home_Starter_adj_fsv'] * .946 + df_players_merged['Home_Backup_adj_fsv'] * .053
    df_teams_merged['away_adj_fsv'] = df_players_merged['Away_Starter_adj_fsv'] * .946 + df_players_merged['Away_Backup_adj_fsv'] * .053
    df_teams_merged['home_b2b'], df_teams_merged['away_b2b'] = df_players_merged['home_b2b'], df_players_merged['away_b2b']

    return df_teams_merged, df_players_merged, df_schedule, df_teams, df_players


def get_probs(date):
    team_model_df, players_model_df, schedule_df, teams_stats_df, player_stats_df = get_team_player_info(date)

    # We can get the player probabilities in bulk, the team depends on the elo so we get them one at a time
    schedule_df['player_prob'] = gmp.get_player_probs(players_model_df)

    # Scale the team model features in bulk - elo isn't scaled so this makes it run much faster
    scaled_team_model_df = gmp.scale_team_feats(team_model_df)

    # Add Team Model Data in and drop the duplicate columns
    schedule_df = pd.concat([schedule_df, scaled_team_model_df], axis=1)
    schedule_df = schedule_df.T.drop_duplicates().T

    sims_df = run_simulations.simulate_season(schedule_df.to_dict("records"), teams_stats_df, player_stats_df, date)

    # Push to db
    engine = create_engine('postgresql://{}:{}@{}:5432/{}'.format(USERNAME, PASSWORD, HOST, SITE_DB))
    sims_df.to_sql('season_projs_seasonprojs', engine, if_exists='append', index=False)


def main():
    get_probs("2019-04-07")


if __name__ == "__main__":
    main()
