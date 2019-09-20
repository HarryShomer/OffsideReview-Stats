import pandas as pd
import datetime
import json
import helpers


def get_json():
    """
    Get the Json of the outcomes
    
    :return: Json of outcomes
    """
    # Get Yesterday's Date
    yesterday = datetime.date.today() - datetime.timedelta(1)
    date = '-'.join([str(yesterday.year), str(yesterday.month), str(yesterday.day)])

    url = f"https://statsapi.web.nhl.com/api/v1/schedule?startDate={date}&endDate={date}&expand=schedule.linescore"

    return json.loads(helpers.get_page(url))


def get_yesterday_outcomes():
    """
    Get Outcomes for games yesterday
    
    :return: 
    """
    outcomes = get_json()

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


def main():
    print(get_yesterday_outcomes())


if __name__ == "__main__":
    main()