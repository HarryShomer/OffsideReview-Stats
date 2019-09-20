import helpers
import json


def get_standings():
    """
    Get the current standing as per the NHL
    
    :return: Json of teams
    """
    response = helpers.get_page("https://statsapi.web.nhl.com/api/v1/standings")

    return json.loads(response)


def parse_json(standings):
    """
    Parse the standing json
    
    :param standings: json of standings
    
    :return: dict 
    """
    team_standings = dict()

    for division in standings['records']:
        for team in division['teamRecords']:
            team_standings[helpers.TEAMS[team['team']['name'].upper()]] = {
                'team': helpers.TEAMS[team['team']['name'].upper()],
                "ROW": team['leagueRecord']['wins'] * 2 + team['leagueRecord']['ot'],
                'points': team['leagueRecord']['wins'] * 2 + team['leagueRecord']['ot'],
                "GD": team['goalsScored'] - team['goalsAgainst'],
                'round_1': 0,
                'round_2': 0,
                'round_3': 0,
                'round_4': 0,
                'champion': 0
            }

    return team_standings


def scrape_todays_standings():
    """
    Scrapes standings as of today
    
    :return: dictionary: keys -> teams, value -> {wins, ot}
    """
    return parse_json(get_standings())


def main():
    print(json.dumps(scrape_todays_standings(), indent=2))


if __name__ == "__main__":
    main()