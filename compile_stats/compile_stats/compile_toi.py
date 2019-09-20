"""
Module used for calculating the TOI for players and teams for a list of games.
1. Needs to be a list of games and the DataFrame full of shifts (needs to include positions).
2. compile_toi.process_games -> starts the whole thing
"""

# I don't think this is needed anymore...Idk
TEAMS = {'TBL': 'T.B', 'LAK': 'L.A', 'NJD': 'N.J', 'SJS': 'S.J'}

# "Approved" Strengths
strengths = [
        '5x5',
        '6x5',
        '5x6',
        '5x4',
        '4x5',
        '5x3',
        '3x5',
        '4x4',
        '4x3',
        '3x4',
        '3x3',
        '6x4',
        '4x6',
        '6x3',
        '3x6',
        '2x2',
]


def combine_games(games):
    """
    Turns toi dictionaries into DataFrames

    :param games: Dictionary that hold date of game, player_toi dict, and team_toi dict

    :return: DataFrame of TOI for teams and players for given games
    """
    players = []
    teams = []

    for game in list(games.keys()):
        for team in list(games[game]['teams'].keys()):
            # Players
            for player in list(games[game]['players'][team].keys()):
                for strength in list(games[game]['players'][team][player]['TOI'].keys()):

                    # TODO: Needed???
                    if team in list(TEAMS.keys()):
                        fixed_team = TEAMS[team]
                    else:
                        fixed_team = team

                    for net in list(games[game]['players'][team][player]['TOI'][strength].keys()):
                        players.append({
                            'player': games[game]['players'][team][player]['Player'],
                            'position': games[game]['players'][team][player]['Position'],
                            'player_id': player,
                            'game_id': game,
                            'date': games[game]['date'],
                            'team': fixed_team,
                            'strength': strength,
                            'if_empty': 1 if net == 'Empty' else 0,
                            'toi_on': games[game]['players'][team][player]['TOI'][strength][net]['On'],
                            'toi_off': games[game]['players'][team][player]['TOI'][strength][net]['Off']

                        })

            # Teams
            for strength in list(games[game]['teams'][team].keys()):
                for net in list(games[game]['teams'][team][strength].keys()):
                    teams.append({
                        'team': team,
                        'game_id': game,
                        'date': games[game]['date'],
                        'strength': strength,
                        'if_empty': 1 if net == 'Empty' else 0,
                        'toi': games[game]['teams'][team][strength][net],
                    })

    return players, teams


def get_player(row):
    return [row['player'], row['player_id'], row['position'], row['team']]


def get_players(game_df):
    """
    Get players who played in specific game
    
    :param game_df: DataFrame for game with shift info
    
    :return: players list
    """
    players_series = game_df.apply(lambda row: get_player(row), axis=1)
    players_set = set(tuple(x) for x in players_series.tolist())

    return [list(x) for x in players_set]


def get_game_length(game_df, game, teams):
    """
    Gets a list with the length equal to the amount of seconds in that game
    
    :param game_df: DataFrame with shift info for game
    :param game: game_id
    :param teams: both teams in game
    
    :return: list 
    """
    # Start off with the standard 3 periods (1201 because start at 0)
    seconds = list(range(0, 1201)) * 3

    # If the last shift was an overtime shift, then extend the list of seconds by how fair into OT the game went
    if game_df['period'][game_df.shape[0] - 1] == 4:
        seconds.extend(list(range(0, game_df['end'][game_df.shape[0] - 1] + 1)))

    # For Playoff Games
    # If the game went beyond 4 periods tack that on to
    if int(game) >= 30000:
        # Go from beyond period 4 because done above
        for i in range(game_df['period'][game_df.shape[0] - 1] - 4):
            seconds.extend(list(range(0, 1201)))

        seconds.extend(list(range(0, game_df['end'][game_df.shape[0] - 1] + 1)))

    # Create dict for seconds_list
    # On = Players on Ice at that second
    # Off = Players who got off ice at that second
    for i in range(len(seconds)):
        seconds[i] = {
            teams[0]: {'On': {'Skaters': dict(), 'Goalies': dict()}, 'Off': {'Skaters': dict(), 'Goalies': dict()}},
            teams[1]: {'On': {'Skaters': dict(), 'Goalies': dict()}, 'Off': {'Skaters': dict(), 'Goalies': dict()}}
        }

    return seconds


def fill_player_dict(teams, players_list):
    """
    Fill by team by player with stuff
    
    NOTE: Ignore the 'team_toi and 'other_team_toi' keys
    
    :param teams: both teams in game
    :param players_list: list of players in game
    
    :return: dict by player by team
    """
    players_toi = {teams[0]: dict(), teams[1]: dict()}

    for p in players_list:
        players_toi[p[3]][str(p[1])] = {
            'Player': p[0],
            'Position': p[2],
            'TOI': {
                '5x5': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '6x5': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '5x6': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '5x4': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '4x5': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '5x3': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '3x5': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '4x4': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '4x3': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '3x4': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '3x3': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '6x4': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '4x6': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '6x3': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                '3x6': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
                # '2x2' is for misc. TOI
                '2x2': {'Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}},
                        'Not_Empty': {'On': 0, 'Off': 0, 'team_toi': {}, 'other_team_toi': {}}},
            }
        }

    return players_toi


def fill_team_dict(teams):
    """
    Fill by team with stuff
    
    :param teams: both teams in game
    
    :return: dict by by team
    """
    teams_toi = {teams[0]: dict(), teams[1]: dict()}
    for t in teams:
        teams_toi[t] = {
            '5x5': {'Empty': 0, 'Not_Empty': 0},
            '6x5': {'Empty': 0, 'Not_Empty': 0},
            '5x6': {'Empty': 0, 'Not_Empty': 0},
            '5x4': {'Empty': 0, 'Not_Empty': 0},
            '4x5': {'Empty': 0, 'Not_Empty': 0},
            '5x3': {'Empty': 0, 'Not_Empty': 0},
            '3x5': {'Empty': 0, 'Not_Empty': 0},
            '4x4': {'Empty': 0, 'Not_Empty': 0},
            '4x3': {'Empty': 0, 'Not_Empty': 0},
            '3x4': {'Empty': 0, 'Not_Empty': 0},
            '3x3': {'Empty': 0, 'Not_Empty': 0},
            '6x4': {'Empty': 0, 'Not_Empty': 0},
            '4x6': {'Empty': 0, 'Not_Empty': 0},
            '6x3': {'Empty': 0, 'Not_Empty': 0},
            '3x6': {'Empty': 0, 'Not_Empty': 0},
            '2x2': {'Empty': 0, 'Not_Empty': 0},  # For misc. TOI...where I store mistakes
        }

    return teams_toi


def distribute_toi(second, seconds_list, players_toi, teams_toi):
    """
    Distributes toi for given second for players and teams
    """
    both_teams = list(seconds_list[second].keys())

    players_on_ice = {
        both_teams[0]: len(seconds_list[second][both_teams[0]]['On']['Skaters'].keys()),
        both_teams[1]: len(seconds_list[second][both_teams[1]]['On']['Skaters'].keys())
    }

    goalies_on_ice = {
        both_teams[0]: len(seconds_list[second][both_teams[0]]['On']['Goalies'].keys()),
        both_teams[1]: len(seconds_list[second][both_teams[1]]['On']['Goalies'].keys())
    }

    # Players for both teams
    for team in list(seconds_list[second].keys()):
        # Determine Strength
        if team == both_teams[0]:
            strength = 'x'.join([str(players_on_ice[team]), str(players_on_ice[both_teams[1]])])
            other_team = both_teams[1]
        else:
            strength = 'x'.join([str(players_on_ice[team]), str(players_on_ice[both_teams[0]])])
            other_team = both_teams[0]

        # Don't bother if it's a shootout
        if strength not in strengths:
            if strength in ['1x1', '0x0', '1x0', '0x1', '1x1']:
                return
            else:
                # There are mistakes with the shift charts ...any misc. strength not in above goes here
                strength = '2x2'

        # Check if empty net
        if goalies_on_ice[both_teams[0]] == 0 or goalies_on_ice[both_teams[1]] == 0:
            if_empty = 'Empty'
        else:
            if_empty = 'Not_Empty'

        # For Players
        for player_key in list(players_toi[team].keys()):
            if players_toi[team][player_key]['Position'] != 'G':
                if player_key in list(seconds_list[second][team]['On']['Skaters'].keys()):
                    players_toi[team][player_key]['TOI'][strength][if_empty]['On'] += 1

                    """
                    ########### Get toi spent with players on other team ##############################
                    ####################################################################################################
                    # Go through players on the other team
                    for opp_player in list(players_toi[other_team].keys()):
                        if opp_player in list(seconds_list[second][other_team]['On']['Skaters'].keys()):
                            if opp_player in list(players_toi[team][player_key]['TOI'][strength][if_empty]['opp_team_toi'].keys()):
                                players_toi[team][player_key]['TOI'][strength][if_empty]['opp_team_toi'][opp_player] += 1
                            else:
                                players_toi[team][player_key]['TOI'][strength][if_empty]['opp_team_toi'][opp_player] = 1
                    ####################################################################################################
                    
                    
                    ################### TOI with players on same team ###############################
                    for same_team_key in list(players_toi[team].keys()):
                        if same_team_key in list(seconds_list[second][team]['On']['Skaters'].keys()) and same_team_key != player_key:
                            if same_team_key in list(players_toi[team][player_key]['TOI'][strength][if_empty]['team_toi'].keys()):
                                players_toi[team][player_key]['TOI'][strength][if_empty]['team_toi'][same_team_key] += 1
                            else:
                                players_toi[team][player_key]['TOI'][strength][if_empty]['team_toi'][same_team_key] = 1
                    ####################################################################################################
                    """
                else:
                    players_toi[team][player_key]['TOI'][strength][if_empty]['Off'] += 1
            else:
                if player_key in list(seconds_list[second][team]['On']['Goalies'].keys()):
                    players_toi[team][player_key]['TOI'][strength][if_empty]['On'] += 1
                else:
                    players_toi[team][player_key]['TOI'][strength][if_empty]['Off'] += 1

        # For Teams
        teams_toi[team][strength][if_empty] += 1


def populate_matrix(row, seconds_list):
    """
    For a given shift it fills in the seconds the player was on the ice
    
    :param row: given shifts info
    :param seconds_list: list with every second of game
    """
    start = row['start']
    end = row['end']
    team = row['team']

    if row['period'] != 1:
        start += (1200 * (row['period'] - 1))
        end += (1200 * (row['period'] - 1))

    for x in range(start, end+1):
        # 'On' really just means the player isn't getting off at that second
        if x == end:
            shift_type = 'Off'
        else:
            shift_type = 'On'

        if row['position'] == 'G':
            seconds_list[x][team][shift_type]['Goalies'][str(row['player_id'])] = {'Player': row['player'],
                                                                                   'Position': row['position']}
        else:
            seconds_list[x][team][shift_type]['Skaters'][str(row['player_id'])] = {'Player': row['player'],
                                                                                   'Position': row['position']}


def process_games(shifts_df):
    """
    Processes the actual games
    
    :param shifts_df: DataFrame of shifts for those games
    
    :return: DataFrame of to for players and teams
    """
    games = dict()

    for game in list(set(shifts_df['game_id'].tolist())):
        print('Calculating TOI for game ' + str(game))

        game_df = shifts_df[shifts_df['game_id'] == game]
        game_df = game_df.sort_values(by=['period', 'end']).reset_index(drop=True)
        date = game_df.iloc[0]['date']

        # Get Players and Teams
        teams = list(set(game_df['team'].tolist()))
        players_list = get_players(game_df)

        # Get game Length
        seconds_list = get_game_length(game_df, game, teams)

        # Fill with base
        players_toi = fill_player_dict(teams, players_list)
        teams_toi = fill_team_dict(teams)

        # Populate seconds_list with players
        shifts = game_df.to_dict('records')
        [populate_matrix(shift, seconds_list) for shift in shifts]

        # Put toi in for players and team
        [distribute_toi(s, seconds_list, players_toi, teams_toi) for s in range(len(seconds_list))]

        games[str(game)] = {'date': date, 'players': players_toi, 'teams': teams_toi}

        # Debugging
        """
        if game == '20107':
            import json
            print(json.dumps(teams_toi, indent=2))
        """

    players, teams = combine_games(games)
    return players, teams
