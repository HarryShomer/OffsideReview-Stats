import json
import pandas as pd
import helpers
from models import player_model, get_goalie_stats as ggs


def rosters_json():
    """
    Get The Json with the current roster info
    
    :return: List of dictionaries (which each contain a teams info)
    """
    response = helpers.get_page("https://statsapi.web.nhl.com/api/v1/teams?expand=team.roster")
    return json.loads(response)['teams']


def construct_roster(skaters, goalies):
    """
    Construct with the top 12 F, 6 defensemen and 2 goalies
    
    :param skaters: Dictionary with lists of marcels for F and D
    :param goalies: List of marcels for goalies
    
    :return: Top 20 in a dictionary with proper naming for the final DataFrame
    """
    # For Game Score when not enough players at a position
    # The actual avrage are 1.5 and .95. This is 85% of them.
    pos_avgs = {'F': 1.275, "D": .8075}

    row = dict()

    # F/D
    for pos in [['F', 12], ['D', 6]]:
        for index in range(0, pos[1]):
            try:
                row['_'.join([pos[0], str(index + 1)])] = skaters['F'][index]['gs']
            except IndexError:
                print("Missing {} #{}".format(pos[0], index + 1))
                row['_'.join([pos[0], str(index + 1)])] = pos_avgs[pos[0]]

    # Assign for initial goalie
    row['Starter'], row['Starter_adj_fsv'] = goalies[0]['goalie'], goalies[0]['adj_fsv']

    # This happens
    if len(goalies) > 1:
        row['Backup'], row['Backup_adj_fsv'] = goalies[1]['goalie'], goalies[1]['adj_fsv']
    else:
        print("Missing Backup")
        row['Backup'], row['Backup_adj_fsv'] = None, 0

    return row


def get_players(date, roster, skater_df, goalie_df):
    """
    Given a roster for a team separate all of the F/D/G. Then get the marcels for each one. We then order them by
    their projected toi and pick the top 12 for F, top 6 for D and top 2 for goalies. 
    
    Calls 'construct_roster' to put it all together
    
    :param date: Today's date
    :param roster: Roster for a given team
    :param skater_df: DataFrame of all stats for skaters
    :param goalie_df: DataFrame of all stats for goalies
    
    :return: 
    """
    from operator import itemgetter

    # Just need id for each
    skaters = {
        "F": [player['person']['id'] for player in roster if player['position']['type'] == "Forward"],
        "D": [player['person']['id'] for player in roster if player['position']['type'] == "Defenseman"]
    }

    # Go with name here...don't ask
    goalies = [helpers.fix_name(player['person']['fullName']).upper() for player in roster if player['position']['type'] == "Goalie"]
    goalie_marcels = ggs.get_marcels(goalies, date, goalie_df)
    goalie_marcels = sorted(goalie_marcels, key=itemgetter('gp'), reverse=True)

    # Sort forwards and defensemen
    skater_marcels = player_model.get_marcels(skaters, date, skater_df)
    skater_marcels['F'] = sorted(skater_marcels['F'], key=itemgetter('toi'), reverse=True)
    skater_marcels['D'] = sorted(skater_marcels['D'], key=itemgetter('toi'), reverse=True)

    # Convert to proper row format for final DataFrame
    return construct_roster(skater_marcels, goalie_marcels)


def get_rosters(date):
    """
    Get Rosters and marcels
    
    :param date: Today's date
    
    :return: rosters
    """
    # Get Stats for Skaters and Goalies
    skater_df = player_model.get_raw_data(date)
    goalie_df = ggs.get_goalie_data(date)

    teams = []
    for team in rosters_json():
        players = get_players(date, team["roster"]["roster"], skater_df, goalie_df)
        players['team'] = helpers.TEAMS[team['name'].upper()]
        teams.append(players)

    return pd.DataFrame(teams).sort_values(by=["team"])


def main():
    get_rosters("2018-08-11")


if __name__ == "__main__":
    main()

