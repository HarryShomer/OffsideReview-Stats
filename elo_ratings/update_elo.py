import os
import json
import game_outcomes


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
    k_rating = 4 + (4 * game['GD']) - (game['if_shootout'] * 2)

    # New Rating = Old Rating + k * (actual - expected)
    elo_change = k_rating * (game['if_home_win'] - game['home_prob'])
    team_elo[game['home_team']] += elo_change
    team_elo[game['away_team']] -= elo_change

    return team_elo


def update_team_elo():
    """
    Update the Elo Ratings given the outcomes of yesterdays games

    :return: None
    """
    # Gets elo ratings as of yesterday
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "elo_ratings_2018.json")) as infile:
        team_elo = json.load(infile)

    df = game_outcomes.get_yesterday_outcomes()

    for row in df.to_dict("records"):
        row['home_prob'] = get_home_prob(row, team_elo)
        team_elo = update_elo(row, team_elo)

    # Write over new Ratings
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "elo_ratings_2018.json"), "w") as outfile:
        json.dump(team_elo, outfile)


def main():
    update_team_elo()

if __name__ == "__main__":
    main()
