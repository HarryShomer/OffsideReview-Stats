import os
import pandas as pd
import json


def get_elo_ratings():
    """
    Get Current Elo Ratings
    
    :return: team_elo dict
    """
    # Change to correct path for elo
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    os.chdir("../../elo_ratings")

    with open("elo_ratings_2018.json") as infile:
        team_elo = json.load(infile)

    os.chdir("..")

    return team_elo


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


def update_elo(game, team_elo):
    """
    Update the elo ratings for both teams after a game

    The k-rating formula is taken from Cole Anderson - http://crowdscoutsports.com/team_elo.php
    """
    # k is the constant for how much the ratings should change from this game
    win_margin = abs(game['GD'])
    if_shootout = 1 if game['win_type'] == "SO" else 0
    k_rating = 4 + (4 * win_margin) - (if_shootout * 2)

    # New Rating = Old Rating + k * (actual - expected)
    elo_change = k_rating * (game['if_home_win'] - game['home_prob'])
    team_elo[game['home_team']] += elo_change
    team_elo[game['away_team']] -= elo_change

    return team_elo


def get_elo(games):
    """
    Iterate through every game and:
    1. Get % of home team winning
    2. Randomly choose the winner
    3. Update Elo using GD and Win type (provided in games)
    """
    # Change to correct path for elo
    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    os.chdir("../../elo_ratings")
    with open("Elo_Ratings_2018.json") as infile:
        team_elo = json.load(infile)
    os.chdir("..")

    games_list = []
    for row in games:
        # Get the Probability of the Home Team winning and update then ratings based on the game outcome and probability
        row['home_prob'] = get_home_prob(row, team_elo)
        team_elo = update_elo(row, team_elo)
        games_list.append(row)

    # Move to prev dir
    os.chdir("..")

    return pd.DataFrame(games_list)


def main():
    games = [
        {"Team": "T.B", "Opponent": "VGK"},
        {"Team": "OTT", "Opponent": "WPG"}
    ]
    df = get_elo(games)
    print(df)


if __name__ == "__main__":
    main()
