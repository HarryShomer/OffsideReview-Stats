"""
File used to start process of compiling stats to insert into site db
"""
import datetime
import pandas as pd
import psycopg2
import sqlalchemy
from psycopg2.extensions import AsIs
import hockey_scraper as hs

# Modules from this project
from nhl_players import process_players, player_info
from xg_probs import goal_probs
import shared
from compile_stats import aggregate_stats, compile_toi, push_to_db as ptd
from coords_adjs import apply_coords_adjustments as aca

import sys
sys.path.append("..")
from machine_info import *


pd.options.mode.chained_assignment = None  # default='warn' -> Stops it from giving me some error


def get_season_table(table, cols, engine):
    """
    Get season table for pbp and shifts
    It's for full season tables -> ex: pbp2017, shifts2017...etc

    :param table: db table
    :param cols: Columns to get from the table
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
        df = pd.concat([chunk for chunk in pd.read_sql_table(table, engine, columns=cols, chunksize=10000)])

    return df


def rink_adjust(pbp):
    """
    Adjust the coordinates for rink bias. Adjusted based on current season and last season's data

    :param pbp: DataFrame of scraped games

    :return: DataFrame of scraped games with coordinates adjusted
    """
    season = shared.get_season(pbp.iloc[0]['Date'])
    engine = sqlalchemy.create_engine('postgresql://{}:{}@{}:5432/nhl_data'.format(USERNAME, PASSWORD, HOST))

    # Convert pbp cols to lowercase...when we get others from db they will be lowercase
    pbp_cols = pbp.columns
    pbp.columns = map(str.lower, pbp.columns)

    # Columns needed for rink adjusting
    rink_adjust_cols = ['date', 'game_id', 'period', 'home_team', 'away_team', 'event', 'xc', 'yc']

    # Get Previous data
    prev_season_df = get_season_table("pbp" + str(int(season)-1), rink_adjust_cols, engine)
    cur_season_df = get_season_table("pbp" + season, rink_adjust_cols, engine)

    # We build the CDF's with last years data and whatever we have so far this year
    total_pbp = pd.concat([pbp[rink_adjust_cols], cur_season_df, prev_season_df])

    # Change columns back to match everything else
    pbp.columns = pbp_cols

    # Create Rink Adjust object
    rink_adjuster = aca.rca.RinkAdjust()

    # CDF's based on this season and previous year!!!
    aca.create_cdfs(aca.fix_df(total_pbp), rink_adjuster)

    # Adjust DataFrame, but only for the games just scraped
    # Use original pbp for this one!!!!!!!!!!!!!!
    aca.adjust_df(pbp, rink_adjuster)

    return pbp


def fix_shifts_df(shiftsDf):
    """
    Fix some issues with the columns for the shiftsDf. Apparently the way it's set up it prefers the data types
    provided when we read in from a csv as opposed to scraping
    """
    # Make Lowercase
    shiftsDf.columns = map(str.lower, shiftsDf.columns)

    shiftsDf.game_id = shiftsDf.game_id.astype(int)
    shiftsDf.period = shiftsDf.period.astype(int)
    shiftsDf.player_id = shiftsDf.player_id.astype(float)
    shiftsDf.start = shiftsDf.start.astype(int)
    shiftsDf.end = shiftsDf.end.astype(int)
    shiftsDf.duration = shiftsDf.duration.astype(int)

    return shiftsDf


def delete_from_site(from_date, to_date):
    """
    Delete data between date ranges for site db
    
    :param from_date: date from
    :param to_date: date to  
    
    :return: None
    """
    conn = psycopg2.connect(host=HOST, database=SITE_DB, user=USERNAME, password=PASSWORD)
    cur = conn.cursor()

    from_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    to_date = datetime.datetime.strptime(to_date, '%Y-%m-%d')
    from_date = "-".join([str(from_date.year), str(from_date.month), str(from_date.day)])
    to_date = "-".join([str(to_date.year), str(to_date.month), str(to_date.day)])

    # Delete from site
    cur.execute(
        """
        DELETE FROM goalies_goalies where date >= %(from_date)s and date <= %(to_date)s;
        DELETE FROM skaters_skaters where date >= %(from_date)s and date <= %(to_date)s;
        DELETE FROM teams_teams where date >= %(from_date)s and date <= %(to_date)s;
        """, {'from_date': from_date, 'to_date': to_date}
    )
    conn.commit()
    cur.close()
    conn.close()


def delete_from_nhl_data(from_date, to_date):
    """
    Delete data between date ranges from nhlData database
    
    :param from_date: date from
    :param to_date: date to  
    """
    conn = psycopg2.connect(host=HOST, database="nhl_data", user=USERNAME, password=PASSWORD)
    cur = conn.cursor()

    # First drop any of the temp tables if there (means there was some error or bec. of debugging
    aggregate_stats.drop_tables(cur, conn)

    year = shared.get_season(to_date)

    # Check if any data yet for that year
    cur.execute("select * from information_schema.tables where table_name=%s", ('pbp' + year,))
    if not bool(cur.rowcount) or cur.rowcount == 0:
        return

    cur.execute(
        """
        -- Delete From pbp and shifts
        DELETE FROM %(pbp)s where CAST (date AS DATE) >= %(from_date)s and CAST (date AS DATE) <= %(to_date)s;
        -- DELETE FROM %(shifts)s where CAST (date AS DATE) >= %(from_date)s and CAST (date AS DATE) <= %(to_date)s;
        """, {'from_date': from_date, 'to_date': to_date, 'pbp': AsIs('pbp' + year), 'shifts': AsIs('shifts' + year)}
    )
    conn.commit()
    cur.close()
    conn.close()


def delete_dates_from_db(from_date, to_date):
    """
    Delete Dates in between those dates from ALL Records
    
    :param from_date: date from
    :param to_date: date to 
    """
    delete_from_site(from_date, to_date)
    print("Deleted any date references from site db")

    delete_from_nhl_data(from_date, to_date)
    print("Deleted any date references from nhl_data db\n")


def process(from_date, to_date):
    """
    Process between dates
    1. Scrape games
    2. Fix up shifts and push new players
    3. Calculate TOI
    4. Push TOI tables, and new pbp and shifts
    """
    # First just delete any previous entries of these dates from every db
    delete_dates_from_db(from_date, to_date)

    scraped_data = hs.scrape_date_range(from_date, to_date, True, data_format='Pandas',
                                        docs_dir="../../hockey_scraper_data")
    pbp_df, shiftsDf = scraped_data['pbp'], scraped_data['shifts']
    season = int(shared.get_season(from_date))

    # Check if scraping went OK
    if pbp_df is None or pbp_df.empty:
        print('Unable to scrape games')
        return ''

    # Fill shifts with positions and push new players to db
    shiftsDf = fix_shifts_df(shiftsDf)
    process_players.process_players(shiftsDf)
    shifts_df = player_info.fill_shifts_with_positions(shiftsDf)

    # Get TOI
    players_toi, teams_toi = compile_toi.process_games(shifts_df)

    # Create DataFrames
    player_df = pd.DataFrame(players_toi, columns=['player', 'player_id', 'position', 'game_id', 'date', 'team',
                                                   'strength', 'if_empty', 'toi_on', 'toi_off'])
    team_df = pd.DataFrame(teams_toi, columns=['team', 'game_id', 'date', 'strength', 'if_empty', 'toi'])

    # Season
    pbp_df['season'] = pbp_df.apply(lambda row: int(shared.get_season(row['Date'])), axis=1)

    # Rink Adjust
    print("\nAdjusting for Rink")
    pbp_df = rink_adjust(pbp_df)

    # Get xG
    pbp_df = goal_probs.get_xg(pbp_df)

    # PUSH all the data to nhl_data DB
    ptd.push_to_db(pbp_df, shiftsDf, player_df, team_df, shared.get_season(pbp_df.iloc[0]['Date']))

    # Aggregate all the statistics and push to site DB
    aggregate_stats.aggregate_all()

    # Return errors for scraping...this is so we can store in logs for daily scraping/compiling
    return scraped_data['errors']
