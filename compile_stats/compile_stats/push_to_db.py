import os
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs

import sys
sys.path.append("../{}".format(os.path.dirname(os.path.realpath(__file__))))
from machine_info import *


def fix_pbp_df(pbpDf):
    """
    Just fix up columns
    """
    # Drop if this column exists
    if 'Unnamed: 0' in list(pbpDf.columns):
        pbpDf = pbpDf.drop(['Unnamed: 0'], axis=1)

    # Make columns lowercase
    pbpDf.columns = map(str.lower, pbpDf.columns)

    # Change type
    pbpDf.game_id = pd.to_numeric(pbpDf.game_id, errors='coerce')
    pbpDf.p1_id = pd.to_numeric(pbpDf.p1_id, errors='coerce')
    pbpDf.p2_id = pd.to_numeric(pbpDf.p2_id, errors='coerce')
    pbpDf.p3_id = pd.to_numeric(pbpDf.p3_id, errors='coerce')
    pbpDf.homeplayer1_id = pd.to_numeric(pbpDf.homeplayer1_id, errors='coerce')
    pbpDf.homeplayer2_id = pd.to_numeric(pbpDf.homeplayer2_id, errors='coerce')
    pbpDf.homeplayer3_id = pd.to_numeric(pbpDf.homeplayer3_id, errors='coerce')
    pbpDf.homeplayer4_id = pd.to_numeric(pbpDf.homeplayer4_id, errors='coerce')
    pbpDf.homeplayer5_id = pd.to_numeric(pbpDf.homeplayer5_id, errors='coerce')
    pbpDf.homeplayer6_id = pd.to_numeric(pbpDf.homeplayer6_id, errors='coerce')
    pbpDf.awayplayer1_id = pd.to_numeric(pbpDf.awayplayer1_id, errors='coerce')
    pbpDf.awayplayer2_id = pd.to_numeric(pbpDf.awayplayer2_id, errors='coerce')
    pbpDf.awayplayer3_id = pd.to_numeric(pbpDf.awayplayer3_id, errors='coerce')
    pbpDf.awayplayer4_id = pd.to_numeric(pbpDf.awayplayer4_id, errors='coerce')
    pbpDf.awayplayer5_id = pd.to_numeric(pbpDf.awayplayer5_id, errors='coerce')
    pbpDf.awayplayer6_id = pd.to_numeric(pbpDf.awayplayer6_id, errors='coerce')
    pbpDf.away_goalie_id = pd.to_numeric(pbpDf.away_goalie_id, errors='coerce')
    pbpDf.home_goalie_id = pd.to_numeric(pbpDf.home_goalie_id, errors='coerce')
    pbpDf.xc = pd.to_numeric(pbpDf.xc, errors='coerce')
    pbpDf.yc = pd.to_numeric(pbpDf.yc, errors='coerce')

    pbpDf.xc_adj = pd.to_numeric(pbpDf.xc_adj, errors='coerce')
    pbpDf.yc_adj = pd.to_numeric(pbpDf.yc_adj, errors='coerce')
    pbpDf.xg = pd.to_numeric(pbpDf.xg, errors='coerce')

    return pbpDf


def csv_to_file(df, df_name):
    """
    Make df into csv file
    
    :param df: DataFrame
    :param df_name: Name of file
    
    :return: None
    """
    # Make Lowercase
    df.columns = map(str.lower, df.columns)
    df.to_csv("{}.csv".format(df_name), sep=',', index=False)


def toi_to_db(cur):
    """

    """
    print("TOI to db")

    # Players
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS player_toi
        (player text, player_id double precision, position text, game_id bigint, date text, team text, strength text,
         if_empty bigint, toi_on double precision, toi_off double precision
        );

        COPY player_toi FROM '{}/player_toi.csv' CSV HEADER;
        """.format(os.getcwd())
    )

    # Teams
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS team_toi
        (team text, game_id bigint, date text, strength text, if_empty bigint, toi double precision);

        COPY team_toi FROM '{}/team_toi.csv' CSV HEADER;
        """.format(os.getcwd())
    )


# TODO: Fix...I guess (read below)
def shifts_to_db(cur, season):
    """
    This doesn't work...probably just an issue with nulls (force change for some cols)
    """

    print("Shifts to db")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS %(shifts)s
        (game_id bigint, period bigint, team text, player text, player_id bigint, start double precision, 
         position text, end double precision, duration double precision, date text
        );

        COPY %(shifts)s FROM %(path)s CSV HEADER;
        """, {'shifts': AsIs('shifts' + str(season)), 'path': os.getcwd() + "/tmp_shifts.csv"}
    )


def pbp_to_db(cur, season):

    print("\nPbp to db")
    for db_type in ['', str(season)]:
        cur.execute(
            """
             CREATE TABLE IF NOT EXISTS %(pbp)s
             (game_id bigint, date text, period int, event text, description text, time_elapsed text,
             seconds_elapsed double precision, strength text, ev_zone text, type text, ev_team text,
             home_zone text, away_team text, home_team text, p1_name text, p1_id double precision, p2_name text,
             p2_id double precision, p3_name text, p3_id double precision, awayplayer1 text, awayplayer1_id double precision,
             awayplayer2 text, awayplayer2_id double precision,  awayplayer3 text,  awayplayer3_id double precision,
             awayplayer4 text, awayplayer4_id double precision,  awayplayer5 text,  awayplayer5_id double precision,
             awayplayer6 text, awayplayer6_id double precision,  homeplayer1 text,  homeplayer1_id double precision,
             homeplayer2 text, homeplayer2_id double precision,  homeplayer3 text,  homeplayer3_id double precision,
             homeplayer4 text, homeplayer4_id double precision,  homeplayer5 text,  homeplayer5_id double precision,
             homeplayer6 text, homeplayer6_id double precision,  away_players bigint, home_players bigint,
             away_score bigint,  home_score bigint, away_goalie text,  away_goalie_id double precision,
             home_goalie text, home_goalie_id double precision,  xc double precision, yc double precision, home_coach text, 
             away_coach text, season bigint, xc_adj double precision, yc_adj double precision, xg double precision,
             reg_xg text, shooter_xg double precision);
    
             COPY %(pbp)s FROM %(path)s CSV HEADER;
            """, {'pbp': AsIs('pbp' + db_type), 'path': os.getcwd() + "/tmp_pbp.csv"}
        )


def push_to_db(pbp, shifts, player_df, team_df, season):
    """
    Push everything to DB:
    1. Team and Player TOI tables
    2. pbp for aggregating data
    3. Add to master pbp
    4. Add to master shifts 
    """
    conn = psycopg2.connect(host=HOST, database="nhl_data", user=USERNAME, password=PASSWORD)
    cur = conn.cursor()

    # Fix misc. columns
    pbp = fix_pbp_df(pbp)
    player_df.game_id = pd.to_numeric(player_df.game_id, errors='coerce')
    player_df.player_id = pd.to_numeric(player_df.player_id, errors='coerce')
    team_df.game_id = pd.to_numeric(team_df.game_id, errors='coerce')

    # Make into CSV files
    csv_to_file(pbp, "tmp_pbp")
    #csv_to_file(shifts, "tmp_shifts")
    csv_to_file(player_df, "player_toi")
    csv_to_file(team_df, "team_toi")


    pbp_to_db(cur, season)
    #shifts_to_db(cur, season)
    toi_to_db(cur)

    conn.commit()
    cur.close()
    conn.close()

    # Delete Tmp Files
    os.remove("tmp_pbp.csv")
    #os.remove("tmp_shifts.csv")
    os.remove("player_toi.csv")
    os.remove("team_toi.csv")


