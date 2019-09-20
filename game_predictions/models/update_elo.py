import os
import json
import helpers
import pandas as pd


def get_json(date):
    """
    Get the Json of the outcomes

    :return: Json of outcomes
    """
    url = f"https://statsapi.web.nhl.com/api/v1/schedule?startDate={date}&endDate={date}&expand=schedule.linescore"
    return json.loads(helpers.get_page(url))


def get_yesterday_outcomes(date):
    """
    Get Outcomes for games yesterday

    :return: 
    """
    outcomes = get_json(date)

    games_list = []
    for date in outcomes['dates']:
        for game in date['games']:
            if int(str(game['gamePk'])[5:]) > 20000:
                games_list.append({
                    "game_id": game['gamePk'],
                    "home_team": helpers.TEAMS[game['teams']['home']['team']['name'].upper()],
                    "away_team": helpers.TEAMS[game['teams']['away']['team']['name'].upper()],
                    "if_shootout": 1 if game['linescore']['hasShootout'] else 0,
                    "GD": abs(game['teams']['away']['score'] - game['teams']['home']['score']),
                    'if_home_win': 1 if game['teams']['home']['score'] - game['teams']['away']['score'] > 0 else 0
                })

    return pd.DataFrame(games_list)


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


def update_team_elo(date):
    """
    Update the Elo Ratings given the outcomes of yesterdays games

    :return: None
    """
    file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../", "elo_ratings/elo_ratings_2018.json")
    # Gets elo ratings as of yesterday
    with open(file) as infile:
        team_elo = json.load(infile)

    df = get_yesterday_outcomes(date)
    for row in df.to_dict("records"):
        row['home_prob'] = get_home_prob(row, team_elo)
        team_elo = update_elo(row, team_elo)

    # Write over new Ratings
    with open(file, "w") as outfile:
        json.dump(team_elo, outfile, indent=4)
