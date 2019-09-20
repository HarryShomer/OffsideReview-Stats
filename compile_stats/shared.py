"""
Shared Functions among different modules
"""

import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

import sys
sys.path.append("..")
from machine_info import *


def get_season_table(table, engine):
    """
    Get season table for pbp and shifts
    It's for full season tables -> ex: pbp2017, shifts2017...etc

    :param table: db table
    :param engine: db connection 

    :return: DataFrame...if not exist return an empty DataFrame
    """
    conn = psycopg2.connect(host=HOST, database="nhl_data", user=USERNAME, password=PASSWORD)
    cur = conn.cursor()

    # If empty...return an empty DataFrame
    cur.execute("select * from information_schema.tables where table_name=%s", (table,))
    if not bool(cur.rowcount) or cur.rowcount == 0:
        df = pd.DataFrame()
    else:
        df = pd.read_sql_table(table, engine, chunksize=10000)

    return df


def get_season(date):
    """
    Get Season based on from_date

    :param date: date

    :return: season -> ex: 2016 for 2016-2017 season
    """
    year = date[:4]
    date = time.strptime(date, "%Y-%m-%d")

    if date > time.strptime('-'.join([year, '01-01']), "%Y-%m-%d"):
        if date < time.strptime('-'.join([year, '09-01']), "%Y-%m-%d"):
            return str(int(year) - 1)
        else:
            return year
    else:
        if date > time.strptime('-'.join([year, '07-01']), "%Y-%m-%d"):
            return year
        else:
            return str(int(year) - 1)


def get_player_handedness(player_id, players):
    """
    Return handedness of player - alerts me if can't find player

    :param player_id: player's id number
    :param players: dict of players

    :return: handedness
    """
    try:
        player_id = int(player_id)
        return players[str(player_id)]["hand"]
    except KeyError:
        print("Player id " + str(player_id) + " not found")
        return ''
    except ValueError:
        return ''


def get_player_position(player_id, players):
    """
    Return position of player - alerts me if can't find player
    ValueError is for when it's nan (not an event with a primary player)

    :param player_id: player's id number
    :param players: dict of players

    :return: position
    """
    try:
        player_id = int(player_id)
        return players[str(player_id)]["pos"]
    except KeyError:
        print("Player id " + str(player_id) + " not found")
        return ''
    except ValueError:
        return ''


def get_player_info():
    """
    Get info on players from db

    :return: dict of players
    """
    engine = create_engine('postgresql://{}:{}@{}:5432/nhl_data'.format(USERNAME, PASSWORD, HOST))
    players_df = pd.read_sql_table('nhl_players', engine)

    # Get list of all players and positions
    players_series = players_df.apply(lambda row: [row['id'], row['shoots_catches'], row['position']], axis=1)
    players_set = set(tuple(x) for x in players_series.tolist())
    players_list = [list(x) for x in players_set]

    # Dict of players -> ID is key
    players = dict()
    for p in players_list:
        players[str(p[0])] = {"hand": p[1], "pos": "F" if p[2] in ["RW", "LW", "C"] else "D"}

    return players


def get_shooter_info(pbp):
    """
    Get info for shooter
    1. Handedness of player
    2. Position of player

    :param pbp: Play by Play

    :return: list containing list of handedness and positions
    """
    players = get_player_info()

    pbp_dict = pbp.to_dict("records")
    shooters_hand = [get_player_handedness(play['p1_ID'], players) for play in pbp_dict]
    shooters_pos = [get_player_position(play['p1_ID'], players) for play in pbp_dict]

    return [shooters_hand, shooters_pos]

