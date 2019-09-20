"""
Elo Ratings should already be updated by external script!!!!!
"""
import os
import json
import pandas as pd


def get_home_prob(game, team_elo):
    """
    Get the probability that the home team will win for a given game

    *** Home Advantage ***
    dr = -400log10(1/prob-1) 
    Home Advantage = 33.5 points. 
    Derived from dr = -400log10(1/prob-1) where prob = .548

    *** Get Home Probability ***
    Prob = 1 / (1 + 10^(dr/400)) ; where dr = difference in ratings plus home bonus.
    dr = -(home_elo - away_elo + 33.5)
    """
    home_advantage = 33.5
    dr = team_elo[game['away_team']] - (team_elo[game['home_team']] + home_advantage)

    return 1 / (1 + 10 ** (dr / 400))


def get_elo(games):
    """
    1. Move to proper path to get updated elo
    2. Get Probs for given games
    """
    # Change to correct path to read in file
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../", "elo_ratings/elo_ratings_2018.json")) as infile:
        team_elo = json.load(infile)

    probs = []
    for game in games:
        probs.append({"game_id": game["game_id"], "home_prob": get_home_prob(game, team_elo)})

    return pd.DataFrame(probs)


def main():
    games = [
        {"home_team": "T.B", "away_team": "VGK", "game_id": 8},
        {"home_team": "OTT", "away_team": "WPG", "game_id": 9}
    ]
    df = get_elo(games)
    print(df)


if __name__ == "__main__":
    main()
