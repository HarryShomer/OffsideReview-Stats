import pandas as pd
import os
import statistics
from operator import itemgetter
import numpy as np
from sklearn.externals import joblib
from models import elo_ratings, get_model_probs as gmp
import todays_standings

# All teams
master_teams = [
        'CAR', 'CBJ', 'N.J', 'NYI', 'NYR', 'PHI', 'PIT', 'WSH', 'BOS', 'BUF', 'DET', 'FLA', 'MTL', 'OTT', 'T.B', 'TOR',
        'CHI', 'COL', 'DAL', 'MIN', 'NSH', 'STL', 'WPG', 'ANA', 'ARI', 'CGY', 'EDM', 'L.A', 'S.J', 'VAN', 'VGK',
]

divisions = {
    "Metro": ['CAR', 'CBJ', 'N.J', 'NYI', 'NYR', 'PHI', 'PIT', 'WSH'],
    'Atlantic': ['BOS', 'BUF', 'DET', 'FLA', 'MTL', 'OTT', 'T.B', 'TOR'],
    'Central': ['CHI', 'COL', 'DAL', 'MIN', 'NSH', 'STL', 'WPG'],
    'Pacific': ['ANA', 'ARI', 'CGY', 'EDM', 'L.A', 'S.J', 'VAN', 'VGK']
}


def merge_players_df(players_df, home_team, away_team):
    """
    Merge to get basic info for both players for that game

    :param players_df: DataFrame of player stats
    :param home_team: Home Team for that game
    :param away_team: Away team for that game

    :return: 
    """
    stat_cols = ['Backup', 'Backup_adj_fsv', 'D_1', 'D_2', 'D_3', 'D_4', 'D_5', 'D_6', 'F_1', 'F_10', 'F_11', 'F_12',
                 'F_2', 'F_3', 'F_4', 'F_5', 'F_6', 'F_7', 'F_8', 'F_9', 'Starter', 'Starter_adj_fsv']

    # Rename and filter columns for merge
    players_df_home = players_df.rename(index=str, columns={col: "Home_" + col for col in stat_cols})
    players_df_home = players_df_home[players_df_home["team"] == home_team].reset_index(drop=True)
    players_df_home = players_df_home.rename(index=str, columns={"team": "home_team"})

    players_df_away = players_df.rename(index=str, columns={col: "Away_" + col for col in stat_cols})
    players_df_away = players_df_away[players_df_away["team"] == away_team].reset_index(drop=True)
    players_df_away = players_df_away.rename(index=str, columns={"team": "away_team"})

    return players_df_home.join(players_df_away)


def merge_teams_df(teams_df, home_team, away_team):
    """
    Merge to get basic info for both teams for that game
    
    :param teams_df: DataFrame of team stats
    :param home_team: Home Team for that game
    :param away_team: Away team for that game
    
    :return: 
    """
    stats_cols = ['PENT60', 'PEND60', 'FF60_even', 'FA60_even', 'xGF60/FF60_even', 'xGA60/FA60_even', 'GF60/xGF60_even',
                  'FF60_pp', 'xGF60/FF60_pp', 'GF60/xGF60_pp', 'FA60_pk', 'xGA60/FA60_pk']

    # Rename and filter columns for merge
    teams_df_home = teams_df.rename(index=str, columns={col: col + "_Team" for col in stats_cols})
    teams_df_home = teams_df_home[teams_df_home["team"] == home_team].reset_index(drop=True)
    teams_df_home = teams_df_home.rename(index=str, columns={"team": "home_team"})

    teams_df_away = teams_df.rename(index=str, columns={col: col + "_Opponent" for col in stats_cols})
    teams_df_away = teams_df_away[teams_df_away["team"] == away_team].reset_index(drop=True)
    teams_df_away = teams_df_away.rename(index=str, columns={"team": "away_team"})

    return teams_df_home.join(teams_df_away)


def randomly_choose_days_of_rest(df, game_num):
    """
    This only used for playoffs. Besides for the first game, the days of rest is obviously the same for both team
    
    So for games 2-7 in a series I go with: 2 Days = 80%, 3 Days = 20%
    For game 1 I go with: 2 Days = 60%, 3 Days = 30%, 4 days = 10%
    
    :param df: DataFrame of info
    :param game_num: Number of game in the series
    
    :return: Df with days of rest for both teams
    """
    if game_num == 1:
        df['days_rest_home'], df['days_rest_away'] = np.random.choice([2, 3, 4], p=[.6, .3, .1]), \
                                                     np.random.choice([2, 3, 4], p=[.6, .3, .1])
    else:
        days_rest = np.random.choice([2, 3], p=[.75, .25])
        df['days_rest_home'], df['days_rest_away'] = days_rest, days_rest

    return df


def get_teams_players_playoffs(teams, team_dfs, player_dfs, game_num):
    """
    For a given game get the team and player data suitable for feeding into their respective models
    
    :param teams: List of both teams -> 0th index is the higher seed
    :param team_dfs: Dict of team model features when both teams are home -> key is home team
    :param player_dfs: Dict of team model features when both teams are home -> key is home team
    :param game_num: Number of game in the series
    
    :return: team_model_df, player_model_df
    """
    # Get home/away based on which game
    if game_num in [1, 2, 5, 7]:
        home_team, away_team = teams[0]['team'], teams[1]['team']
    else:
        home_team, away_team = teams[1]['team'], teams[0]['team']

    # It will be the one with the home team as the key
    teams_df = team_dfs[home_team]
    players_df = player_dfs[home_team]

    # Days of Rest - if 1st game of series they differ among the teams (obv. same from 2-7)
    teams_df = randomly_choose_days_of_rest(teams_df, game_num)

    # Give game_id of 30000 to each - > for choosing starter
    players_df['game_id'] = 2000030000
    players_df = gmp.choose_starter(players_df)

    # No B2B's in the playoffs
    players_df['home_b2b'], players_df['away_b2b'] = 0, 0
    teams_df['home_b2b'], teams_df['away_b2b'] = 0, 0

    # Don't change this. It's the only way I could get it to work
    teams_df['home_adj_fsv'] = (players_df['Home_Starter_adj_fsv'] * .946 + players_df['Home_Backup_adj_fsv'] * .053)[0]
    teams_df['away_adj_fsv'] = (players_df['Away_Starter_adj_fsv'] * .946 + players_df['Away_Backup_adj_fsv'] * .053)[0]

    return teams_df, players_df


def randomly_choose_gd(if_playoff):
    """
    Randomly chooses the Goal Differential (gd) for a game if it didn't go to OT. Depends on RegularSeason/Playoffs
    
    :param: if_playoff: Boolean - If it's the playoffs. 
    
    :return: Randomly chosen Goal Differential
    """
    if if_playoff:
        gd_list = list(range(1, 8))
        gd_probs = [.341, .268, .215, .098, .044, .024, .01]
    else:
        gd_list = list(range(1, 10))
        gd_probs = [.286, .281, .277, .102, .037, .013, .003, .0007, .0003]

    return np.random.choice(gd_list, p=gd_probs)


def randomly_choose_win_type(if_playoff):
    """
    Randomly chooses the type of win:
    1. Regular Season: Regulation, OT, SO
    2. Playoffs: Regulation, OT
    
    :param if_playoff: It varies
    
    :return: 
    """
    if if_playoff:
        win_types = ['Regulation', 'OT']
        win_types_probs = [.77, .23]
    else:
        win_types = ['Regulation', 'OT', 'SO']
        win_types_probs = [.77, .15, .08]

    return np.random.choice(win_types, p=win_types_probs)


def randomly_choose_winner(home_prob):
    """
    Random;y choose the winner of a game
    
    :param home_prob: probability of home team winning
    
    :return: 1 = Win, 0 = Loss for Home Team
    """
    return np.random.choice([0, 1], p=[1-home_prob, home_prob])


def distribute_regular_game(game, teams):
    """
    Distribute results from a regular season game
    
    Distribute Points, ROW, and GD
    
    :param game: game info
    :param teams: team info for that season
    
    :return: None
    """
    # Get winning and losing teams - makes the rest cleaner
    if game['if_home_win']:
        winner = game['home_team']
        loser = game['away_team']
    else:
        winner = game['away_team']
        loser = game['home_team']

    teams[winner]['points'] += 2
    teams[winner]['GD'] += game['GD']
    teams[loser]['GD'] -= game['GD']

    # Loser Points:
    if game['win_type'] in ['SO', 'OT']:
        teams[loser]['points'] += 1

    # ROW doesn't involve SO
    if game['win_type'] in ['Regulation', 'OT']:
        teams[winner]['ROW'] += 1


def playoff_seeding(teams):
    """
    Get the seeding for the playoffs
    
    The way it works is we get the top 3 for every division. We then get the next two best teams from the conference
    for the wild cards. The 2nd and 3rd team in each division play each other. The 1st seed in each division plays one 
    of the wild card teams (the better team plays the lower seed). The 2nd round is also played in division, so the 
    1st vs. Wild Card play the 2 vs 3 in round 2. From there the rest is obvious. 
    
    :param teams: Team and Regular Season results
    
    :return: seeds by division
    """
    playoff_seeds = {
        "metro": [],
        "atlantic": [],
        "central": [],
        "pacific": []
    }

    # Get teams by division first
    team_divisions = {
        'metro': [team for team in teams if team['team'] in divisions['Metro']],
        'atlantic': [team for team in teams if team['team'] in divisions['Atlantic']],
        'central': [team for team in teams if team['team'] in divisions['Central']],
        'pacific': [team for team in teams if team['team'] in divisions['Pacific']]
    }

    # Sort in "proper" order
    for division in team_divisions.keys():
        team_divisions[division] = sorted(team_divisions[division], key=itemgetter('points', 'ROW', 'GD'), reverse=True)
        playoff_seeds[division] = team_divisions[division][:3]

    # Get wild cards for conference
    for conf in [['metro', 'atlantic'], ['central', 'pacific']]:
        conf_remain = team_divisions[conf[0]][3:] + team_divisions[conf[1]][3:]
        wild_cards = sorted(conf_remain, key=itemgetter('points', 'ROW', 'GD'), reverse=True)[:2]

        # Rank the top seeds in both division
        one_seeds = sorted([playoff_seeds[conf[0]][0], playoff_seeds[conf[1]][0]], key=itemgetter('points', 'ROW', 'GD'), reverse=True)

        # The better 1 seed gets the lower wild card
        if one_seeds[0] == team_divisions[conf[0]][0]:
            playoff_seeds[conf[0]].append(wild_cards[1])
            playoff_seeds[conf[1]].append(wild_cards[0])
        else:
            playoff_seeds[conf[0]].append(wild_cards[0])
            playoff_seeds[conf[1]].append(wild_cards[1])

    return playoff_seeds


def sim_playoff_series(teams, team_stats_df, player_stats_df, team_elo, meta_clf):
    """
    Simulate a playoff series and return the winner
    
    :param teams: list of teams -> the 0th entry is the higher seed
    :param team_stats_df: DataFrame of all team stats
    :param player_stats_df: DataFrame of all player stats
    :param: team_elo: Dictionary of elo ratings
    :param: meta_clf: Meta Classifier
    
    :return: Winner of series
    """
    wins = {teams[0]['team']: 0, teams[1]['team']: 0}

    # Get the possibilities for the home and player feature DataFrames
    # The keys is the home team for that game
    team_dfs = {
        teams[0]['team']: merge_teams_df(team_stats_df, teams[0]['team'], teams[1]['team']),
        teams[1]['team']: merge_teams_df(team_stats_df, teams[1]['team'], teams[0]['team'])
    }
    player_dfs = {
        teams[0]['team']: merge_players_df(player_stats_df, teams[0]['team'], teams[1]['team']),
        teams[1]['team']: merge_players_df(player_stats_df, teams[1]['team'], teams[0]['team'])
    }

    for game_num in range(1, 8):
        team_model_df, player_model_df = get_teams_players_playoffs(teams, team_dfs, player_dfs, game_num)

        # Easier to deal with game info like this
        game = {'home_team': team_model_df.iloc[0]['home_team'], 'away_team': team_model_df.iloc[0]['away_team']}

        # Scale model and convert to dictionary
        team_model_df = gmp.scale_team_feats(team_model_df)
        team_model_dict = team_model_df.to_dict("records")[0]

        # Get Prob%  for game
        team_model_dict['elo_prob'] = game['elo_prob'] = elo_ratings.get_home_prob(game, team_elo)
        game['team_prob'] = gmp.get_team_probs(team_model_dict)
        game['player_prob'] = gmp.get_player_probs(player_model_df)[0]

        # Get and assign final prob
        probs = meta_clf.predict_proba([[game['team_prob'], game['player_prob']]])
        game['home_prob'] = probs[0][1]

        game['if_home_win'] = randomly_choose_winner(game['home_prob'])
        game['win_type'] = randomly_choose_win_type(True)

        # Random GD is only for Regulation wins. For OT and OS it's always 1
        game['GD'] = randomly_choose_gd(True) if game['win_type'] == 'Regulation' else 1

        # Update Elo given all info
        team_elo = elo_ratings.update_elo(game, team_elo)

        # Distribute wins for series
        if game['if_home_win']:
            wins[game['home_team']] += 1
        else:
            wins[game['away_team']] += 1

        # If we've hit 4 for one of them we break
        if wins[game['home_team']] == 4 or wins[game['away_team']] == 4:
            break

    return teams[0]['team'] if wins[teams[0]['team']] == 4 else teams[1]['team']


def sim_playoffs(teams, team_stats_df, player_stats_df, team_elo, playoff_seeds):
    """
    Simulate playoffs
    
    :param teams: Regular Season results 
    :param team_stats_df: DataFrame for team stats for each team
    :param player_stats_df: DataFrame for player stats for each team
    :param team_elo: Elo Ratings for this simmed season
    :param playoff_seeds: Seeds of teams for playoffs
    
    :return: Dictionary of Playoff Results
    """
    # Get Meta Classifier and Team Scaler and Classifier
    meta_clf = joblib.load(os.path.join(os.path.dirname(os.path.realpath(__file__)), "clfs/meta_classifier.pkl"))

    # Convert from list of dict to dictionary with key = team
    teams = {key['team']: key for key in teams}

    # Get Winners of each division
    division_winners = dict()
    for division in ['metro', 'atlantic', 'central', 'pacific']:
        # Get Results of first round
        first_round_winners = []
        for matchup in [[0, 3], [1, 2]]:
            # Count each team as making the playoffs
            teams[playoff_seeds[division][matchup[0]]]['round_1'] = 1
            teams[playoff_seeds[division][matchup[1]]]['round_1'] = 1

            # Set up proper seeding and sim
            series_teams = [{"team": playoff_seeds[division][matchup[0]]},
                            {"team": playoff_seeds[division][matchup[1]]}]

            winner = sim_playoff_series(series_teams, team_stats_df, player_stats_df, team_elo, meta_clf)

            # Add info in as team and their seed for proper seeding next round
            winner_seed = matchup[0] + 1 if winner == playoff_seeds[division][matchup[0]] else matchup[1] + 1
            first_round_winners.append({"team": winner, "seed": winner_seed})

        # First count both teams as making it to the 2nd round
        teams[first_round_winners[0]['team']]['round_2'] = 1
        teams[first_round_winners[1]['team']]['round_2'] = 1

        # Get winner of division - 2nd round
        series_teams = sorted(first_round_winners, key=itemgetter('seed'), reverse=True)
        division_winners[division] = sim_playoff_series(series_teams, team_stats_df, player_stats_df, team_elo, meta_clf)

    # Get Winners of the Conference Finals
    # The higher overall seed is the home team for that series
    # Metro vs. Atlantic and Central vs. Pacific
    conf_winners = []
    for conference in [['metro', 'atlantic'], ['central', 'pacific']]:
        # Count teams as making it to conference finals
        teams[division_winners[conference[0]]]['round_3'] = 1
        teams[division_winners[conference[1]]]['round_3'] = 1

        # Get The regular season results for the winner and then sort them to get the proper seeding
        conf_teams = [teams[division_winners[conference[0]]], teams[division_winners[conference[1]]]]
        conf_teams = sorted(conf_teams, key=itemgetter('points', 'ROW', 'GD'), reverse=True)
        conf_winners.append(sim_playoff_series(conf_teams, team_stats_df, player_stats_df, team_elo, meta_clf))

    # Simulate Stanley Cup Final
    final_teams = [teams[conf_winners[0]], teams[conf_winners[1]]]
    final_teams = sorted(final_teams, key=itemgetter('points', 'ROW', 'GD'), reverse=True)
    finals_winner = sim_playoff_series(final_teams, team_stats_df, player_stats_df, team_elo, meta_clf)

    # Count teams as making finals
    teams[final_teams[0]['team']]['round_4'] = 1
    teams[final_teams[1]['team']]['round_4'] = 1

    # Also count team as winner
    teams[finals_winner]['champion'] = 1

    return teams


def sim_regular_season(teams_season, games, team_elo):
    """
    Simulate all the regular season games
    
    :param teams_season: info on each team
    :param games: Regular Season games -> Includes SCALED Team Data
    :param team_elo: Elo ratings
    
    :return: Dictionary of season results
    """
    meta_clf = joblib.load(os.path.join(os.path.dirname(os.path.realpath(__file__)), "clfs/meta_classifier.pkl"))

    for game in games:
        # Get Elo and add into Team model Data and the regular game dictionary
        game['elo_prob'] = elo_ratings.get_home_prob(game, team_elo)

        # Get Team and then meta probs
        game['team_prob'] = gmp.get_team_probs(game)
        probs = meta_clf.predict_proba([[game['team_prob'], game['player_prob']]])
        game['home_prob'] = probs[0][1]

        game['if_home_win'] = randomly_choose_winner(game['home_prob'])
        game['win_type'] = randomly_choose_win_type(False)

        # Random GD is only for Regulation wins. For OT and OS it's always 1
        game['GD'] = randomly_choose_gd(False) if game['win_type'] == 'Regulation' else 1

        # Update Elo given all info
        team_elo = elo_ratings.update_elo(game, team_elo)

        # Distribute info for season
        distribute_regular_game(game, teams_season)

    return teams_season


def simulate_season(games, team_stats_df, player_stats_df, date):
    """
    Data included in game -> Home/Away Team, Probabilities from team and player model
    
    NOTE: Average time is 8 seconds per season
    
    :param games: Dictionary of All Games and team model data
    :param team_stats_df: Team stats for season
    :param player_stats_df: Player Stats for season
    :param date: Today's date
    
    :return: List of projections for each team
    """
    # Dictionary to hold all team info
    # Rounds 1-4 is *getting* to that round
    # Elements in list hold dict like -> {'points', 'ROW' 'GD', 'round_1', 'round_2', 'round_3', 'round_4', 'champion'}
    team_sims = {team: [] for team in master_teams}

    # Get standings as of today
    standings = todays_standings.scrape_todays_standings()

    for i in range(5000):
        print(i)
        team_elo = elo_ratings.get_elo_ratings()

        # Check the upcoming schedule to see if have any regular season games
        # If not we know the regular season is over so we don't sim it
        if [game['game_id'] for game in games if int(str(game['game_id'])[5:]) < 30000]:
            season_sim = sim_regular_season(standings, games, team_elo)
            season_sim = sorted([season_sim[key] for key in season_sim.keys()], key=itemgetter('points'), reverse=True)

        sim_results = sim_playoffs(season_sim, team_stats_df, player_stats_df, team_elo, playoff_seeding(season_sim))

        # Add to master list
        for team in sim_results:
            team_sims[team].append(sim_results[team])

    final_results = combine_seasons(team_sims, date)

    #print(json.dumps(final_results, indent=2))

    return pd.DataFrame(final_results)


def combine_seasons(teams, date):
    """
    Combine the season simulations to get:
    1. Points - Mean and std.
    2. Probability of every playoff step 
    
    {'points', 'ROW' 'GD', 'round_1', 'round_2', 'round_3', 'round_4', 'champion'}
    
    :param teams: Dictionary of every team -> value is list of every season sim
    :param date: Today's date
    
    :return: Return List of every team with avg/std/probabilities
    """
    combined_teams = []

    for team in teams:
        points, round_1, round_2, round_3, round_4, champion = [], [], [], [], [], [],
        for sim in teams[team]:
            points.append(sim['points'])
            round_1.append(sim['round_1'])
            round_2.append(sim['round_2'])
            round_3.append(sim['round_3'])
            round_4.append(sim['round_4'])
            champion.append(sim['champion'])

        combined_teams.append({
            "primary_key": "_".join([team, date]),
            "team": team,
            "date": date,
            "points_avg": sum(points) / len(points),
            "points_std": statistics.stdev(points),
            "round_1_prob": sum(round_1) / len(round_1),
            "round_2_prob": sum(round_2) / len(round_2),
            "round_3_prob": sum(round_3) / len(round_3),
            "round_4_prob": sum(round_4) / len(round_4),
            "champion_prob": sum(champion) / len(champion),
        })

    return combined_teams

