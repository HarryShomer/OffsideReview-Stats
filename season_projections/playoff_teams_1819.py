"""
Playoff teams for the 2018-2019 season
"""
import pandas as pd
import json
import statistics
import run_simulations
from models import team_model, elo_ratings, get_team_rosters as gtr
from todays_standings import scrape_todays_standings
from time import time
import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

DATE = "2019-04-07"
PLAYOFF_SEEDS = {
    "metro": ["WSH", "NYI", "PIT", "CAR"],
    "atlantic": ["T.B", "BOS", "TOR", "CBJ"],
    "central": ["NSH", "WPG", "STL", "DAL"],
    "pacific": ["CGY", "S.J", "VGK", "COL"]
}
PLAYOFF_TEAMS = [t for key in PLAYOFF_SEEDS for t in PLAYOFF_SEEDS[key]]
STANDINGS = scrape_todays_standings()


def get_models():
    print("Loading models", end="", flush=True)
    t = pd.read_csv("team_stats_df.csv", index_col=0)
    print(".", end="", flush=True)
    p = pd.read_csv("player_stats_df.csv", index_col=0)
    print(".", end="", flush=True)
    e = elo_ratings.get_elo_ratings()
    print(". Done")
    return t, p, e


def get_playoff_teams():
    return [STANDINGS[team] for team in STANDINGS if team in PLAYOFF_TEAMS]

# Load models
team_stats_df, player_stats_df, team_elo = get_models()

####################
#   Sim playoffs   #
#  5.16 secs each  #
####################
team_sims = {team: [] for team in PLAYOFF_TEAMS}
for i in range(10000):
    print(i)
    playoff_sim = json.loads(json.dumps(get_playoff_teams()))  # So not same copy
    sim_results = run_simulations.sim_playoffs(playoff_sim, team_stats_df, player_stats_df, team_elo, PLAYOFF_SEEDS)

    # Add to master list
    for team in sim_results:
        team_sims[team].append(sim_results[team])


####################
#   Combine Data   #
####################
combined_teams = []
for team in team_sims:
    points, round_1, round_2, round_3, round_4, champion = [], [], [], [], [], []
    for sim in team_sims[team]:
        points.append(sim['points'])
        round_1.append(sim['round_1'])
        round_2.append(sim['round_2'])
        round_3.append(sim['round_3'])
        round_4.append(sim['round_4'])
        champion.append(sim['champion'])

    combined_teams.append({
        "primary_key": "_".join([team, DATE]),
        "team": team,
        "date": DATE,
        "points_avg": sum(points) / len(points),
        "points_std": statistics.stdev(points),
        "round_1_prob": sum(round_1) / len(round_1),
        "round_2_prob": sum(round_2) / len(round_2),
        "round_3_prob": sum(round_3) / len(round_3),
        "round_4_prob": sum(round_4) / len(round_4),
        "champion_prob": sum(champion) / len(champion),
    })

# Add non-playoff teams
for team in STANDINGS:
    if team not in PLAYOFF_TEAMS:
        combined_teams.append({
            "primary_key": "_".join([team, DATE]),
            "team": team,
            "date": DATE,
            "points_avg": STANDINGS[team]['points'],
            "points_std": 0,
            "round_1_prob": 0,
            "round_2_prob": 0,
            "round_3_prob": 0,
            "round_4_prob": 0,
            "champion_prob": 0,
        })

with open("playoff_sim_results.json", "w") as f:
    json.dump(combined_teams, f, indent=4)
