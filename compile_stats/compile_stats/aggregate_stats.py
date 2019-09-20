import psycopg2
import sys
sys.path.append("..")
from machine_info import *


def add_goalies_to_web(cur_web):
    """
    Adds new goalie data to website
    :param cur_web: 
    """
    cur_web.execute(
        """
        CREATE EXTENSION dblink;
        
        INSERT INTO goalies_goalies
        SELECT * 
        FROM dblink('dbname=nhl_data port=5432 host={}
                user={} password={}', 'SELECT * FROM goalie_stats')
        AS t(player TEXT, player_id TEXT, game_id SMALLINT, season SMALLINT, date DATE, team TEXT, opponent TEXT, home TEXT, 
        strength TEXT, shots_a SMALLINT, goals_a SMALLINT, fenwick_a SMALLINT, xg_a REAL, corsi_a SMALLINT, toi_on BIGINT,
        toi_off BIGINT, shots_a_sa REAL, fenwick_a_sa REAL, corsi_a_sa REAL, if_empty SMALLINT, primary_key TEXT,
        shooter_xg_a REAL);
        """.format(HOST, USERNAME, PASSWORD)
    )


def add_teams_to_web(cur_web):
    """
    Adds new team data to website
    :param cur_web: 
    """
    cur_web.execute(
        """
        -- CREATE EXTENSION dblink;

        INSERT INTO teams_teams
        SELECT * 
        FROM dblink('dbname=nhl_data port=5432 host={}
                user={} password={}', 'SELECT * FROM team_stats')
        AS t(team TEXT, game_id SMALLINT, season SMALLINT, date DATE,  opponent TEXT, home TEXT, strength TEXT, toi BIGINT,
        shots_f SMALLINT, goals_f SMALLINT, fenwick_f SMALLINT, xg_f REAL, corsi_f SMALLINT,  shots_a SMALLINT, 
        goals_a SMALLINT, fenwick_a SMALLINT, xg_a REAL, corsi_a SMALLINT, pent SMALLINT, pend SMALLINT, gives SMALLINT,
        takes SMALLINT, hits_f SMALLINT, hits_a SMALLINT, face_w SMALLINT, face_l SMALLINT, face_Off SMALLINT, 
        face_Def SMALLINT, face_Neu SMALLINT, shots_f_sa REAL, fenwick_f_sa REAL, corsi_f_sa REAL, shots_a_sa REAL, 
        fenwick_a_sa REAL, corsi_a_sa REAL, if_empty SMALLINT, primary_key TEXT, shooter_xg_a REAL, shooter_xg_f REAL);
        """.format(HOST, USERNAME, PASSWORD)
    )


def add_skaters_to_web(cur_web):
    """
    Adds new team data to website
    :param cur_web: 
    """
    cur_web.execute(
        """
        -- CREATE EXTENSION dblink;

        INSERT INTO skaters_skaters
        SELECT * 
        FROM dblink('dbname=nhl_data port=5432 host={}
                user={} password={}', 'SELECT * FROM skater_stats')
        AS t(player TEXT, player_id BIGINT, position TEXT, handedness TEXT, season SMALLINT, game_id SMALLINT, date DATE,
        team TEXT, opponent TEXT, home TEXT, strength TEXT, toi_on BIGINT, goals SMALLINT, a1 SMALLINT, a2 SMALLINT, 
        isf SMALLINT, ifen SMALLINT, ixg REAL, icors SMALLINT, iBlocks SMALLINT, pen_drawn SMALLINT, pen_taken SMALLINT,
        gives SMALLINT, takes SMALLINT, hits_f SMALLINT, hits_a SMALLINT, ifac_win SMALLINT, ifac_loss SMALLINT, 
        shots_f SMALLINT, goals_f SMALLINT, fenwick_f SMALLINT, xg_f REAL, corsi_f SMALLINT, shots_a SMALLINT,
        goals_a SMALLINT, fenwick_a SMALLINT, xg_a REAL, corsi_a SMALLINT, shots_f_sa REAL, fenwick_f_sa REAL, 
        corsi_f_sa REAL, shots_a_sa REAL, fenwick_a_sa REAL, corsi_a_sa REAL, face_off SMALLINT, face_def SMALLINT,
        face_neu SMALLINT, toi_off BIGINT, shots_f_off SMALLINT, goals_f_off SMALLINT, fenwick_f_off SMALLINT, 
        xg_f_off REAL, corsi_f_off SMALLINT, shots_a_off SMALLINT, goals_a_off SMALLINT, fenwick_a_off SMALLINT, 
        xg_a_off REAL, corsi_a_off SMALLINT, shots_f_off_sa REAL, fenwick_f_off_sa REAL, corsi_f_off_sa REAL, 
        shots_a_off_sa REAL, fenwick_a_off_sa REAL, corsi_a_off_sa REAL, face_off_off SMALLINT, face_neu_off SMALLINT, 
        face_def_off SMALLINT,  if_empty SMALLINT, primary_key TEXT, shooter_xg_a REAL, shooter_xg_a_off REAL, 
        shooter_xg_f REAL, shooter_xg_f_off REAL, shooter_ixg REAL)
        """.format(HOST, USERNAME, PASSWORD)
    )


def drop_tables(cur, conn):
    """
    Drops TOI tables created 
    """
    cur.execute(
        """
        DROP TABLE IF EXISTS team_toi;
        DROP TABLE IF EXISTS player_toi;
        DROP TABLE IF EXISTS goalie_stats;
        DROP TABLE IF EXISTS team_stats;
        DROP TABLE IF EXISTS skater_stats;
        DROP TABLE IF EXISTS pbp;
        """
    )

    conn.commit()


def add_empty_net_column(cur, conn):
    """
    Add column indicating if empty net for both teams
    case when event = 'GOAL' AND ev_team = home_team then 1 else 0 end)
    """
    cur.execute(
        """
        ALTER TABLE pbp ADD COLUMN if_empty SMALLINT;
        
        UPDATE pbp
        SET if_empty = CASE WHEN (home_goalie_id IS NULL or away_goalie_id IS NULL) THEN 1 ELSE 0 END;
        """
    )
    conn.commit()


def add_score_column(cur, conn):
    """
    Adds score_diff columns -3 -> 3+ to pbp
    """

    cur.execute(
        """
        ALTER TABLE pbp ADD COLUMN score_diff SMALLINT;
        

        UPDATE pbp
        SET score_diff = home_score - away_score;
        
        -- Make bins for +3 and -3
        UPDATE pbp
        SET score_diff = CASE WHEN score_diff > 3 THEN 3 ELSE score_diff END;
        
        UPDATE pbp
        SET score_diff = CASE WHEN score_diff < -3 THEN - 3 ELSE score_diff END;
        """
    )

    conn.commit()


def aggregate_goalies(cur, conn):
    """
    Aggregate Stats for goalies
    """

    commands = (

        """ 
        DROP TABLE IF EXISTS home;
        CREATE TABLE home
        AS
        SELECT 
        MAX(home_goalie) as player,
        home_goalie_id as player_id,
        game_id,
        max(date) as date,
        max(home_team) as team,
        max(away_team) as opponent,
        max(home_team) as home,
        strength,
        score_diff,
        sum(case when event IN ('GOAL', 'SHOT') AND ev_team = away_team then 1 else 0 end) Shots_a,
        sum(case when event = 'GOAL' AND ev_team = away_team then 1 else 0 end) Goals_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then 1 else 0 end) fenwick_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then xg else 0 end) xg_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then shooter_xg else 0 end) shooter_xg_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS', 'BLOCK') AND ev_team = away_team then 1 else 0 end) corsi_a,
        if_empty
        FROM pbp 
        WHERE period != 5
        GROUP BY home_goalie_id, game_id, strength, score_diff, if_empty;
        """,

        """
        DROP TABLE IF EXISTS away;
        CREATE TABLE away
        AS
        SELECT 
        MAX(away_goalie) as player,
        away_goalie_id as player_id,
        game_id,
        max(date) as date,
        max(away_team) as team,
        max(home_team) as opponent,
        max(home_team) as home,
        strength,
        score_diff,
        sum(case when event IN ('GOAL', 'SHOT') AND ev_team = home_team then 1 else 0 end) Shots_a,
        sum(case when event = 'GOAL' AND ev_team = home_team then 1 else 0 end) Goals_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then 1 else 0 end) fenwick_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then xg else 0 end) xg_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then shooter_xg else 0 end) shooter_xg_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS', 'BLOCK') AND ev_team = home_team then 1 else 0 end) corsi_a,
        if_empty
        FROM pbp
        WHERE period != 5
        GROUP BY away_goalie_id, game_id, strength, score_diff, if_empty;
        """,

        """
        -- Strength in pbp is 'home_num x away_num'...would need to flip this in away
        -- to get them on the same scale
        UPDATE away
        SET strength = SUBSTR(strength, 3, 1) || 'x' || SUBSTR(strength, 1, 1);
        """,

        """
        DROP TABLE IF EXISTS tmp_stats;
        CREATE TEMP TABLE tmp_stats
        AS
        SELECT * FROM home
        UNION
        SELECT * FROM away;
        """,

        """
        DROP TABLE home;
        DROP TABLE away;
        """,

        """
        -- Delete where name = null
        DELETE from tmp_stats
        WHERE
        player IS NULL or player_id IS NULL;
        """,

        """
        -- Add TOI
        ALTER TABLE tmp_stats ADD COLUMN toi_on BIGINT, ADD COLUMN toi_off BIGINT;
        UPDATE tmp_stats
        SET toi_on = player_toi.toi_on,
            toi_off = player_toi.toi_off
        FROM player_toi
        WHERE
        player_toi.player_id  = tmp_stats.player_id
        AND player_toi.game_id = tmp_stats.game_id
        AND player_toi.strength = tmp_stats.strength
        AND player_toi.if_empty = tmp_stats.if_empty;
        """,

        """
        -- Add home team and opponent to TOI b4 adding it into master
        ALTER TABLE player_toi ADD COLUMN home TEXT, ADD COLUMN opponent TEXT;
        
        UPDATE player_toi p
        SET 
            home = t.home,
            opponent = t.opponent
        FROM tmp_stats t
        WHERE
        p.player_id = t.player_id  
        AND p.date = t.date;
        """,

        """
        -- Insert those with '2x2' (NHL fucked up on shifts so undefined strength) into table
        
        insert into tmp_stats (player, player_id, game_id, date, team, home, opponent, strength, if_empty, toi_on, toi_off)
        select 
        p.player, p.player_id, p.game_id, p.date, p.team, p.home, p.opponent, p.strength, p.if_empty, p.toi_on, p.toi_off
        from
        player_toi p
        WHERE NOT EXISTS (
            SELECT * from tmp_stats t 
            WHERE t.player_id = p.player_id
            AND t.date = p.date
            AND t.strength = p.strength
            AND t.if_empty = p.if_empty
        )
        AND
        p.position = 'G'
        AND
        p.toi_on > 0;
        """,

        """
        -- Get rid useless rows (corsi_a is for shootouts)
        DELETE FROM tmp_stats
        WHERE toi_on IS NULL AND toi_off IS NULL AND corsi_a = 0;
        """,

        """
        -- Mark if home or away team and make tmp_strength
        ALTER TABLE tmp_stats ADD COLUMN home_or_away TEXT, ADD COLUMN tmp_strength TEXT;
        
        UPDATE tmp_stats
        SET 
            home_or_away = CASE WHEN team = home THEN 'home' ELSE 'away' END,
            tmp_strength = CASE WHEN strength = '5x5' THEN '5v5' ELSE 'All' END;
        """,

        """
        ALTER TABLE tmp_stats 
        ADD COLUMN shots_a_sa REAL,
        ADD COLUMN fenwick_a_sa REAL,
        ADD COLUMN corsi_a_sa REAL;
        """,

        """
        UPDATE tmp_stats
        SET
        shots_a_sa = shots_a * (SELECT shots FROM score_coefficients WHERE tmp_stats.home_or_away != team 
                                AND tmp_stats.score_diff = score_coefficients.scorediff),
        fenwick_a_sa = fenwick_a * (SELECT fenwick FROM score_coefficients WHERE tmp_stats.home_or_away != team 
                                    AND tmp_stats.score_diff = score_coefficients.scorediff),
        corsi_a_sa = corsi_a * (SELECT corsi FROM score_coefficients WHERE tmp_stats.home_or_away != team 
                                AND tmp_stats.score_diff = score_coefficients.scorediff);
        """,

        """
        -- Get rid of useless columns
        ALTER TABLE tmp_stats DROP COLUMN home_or_away, DROP COLUMN tmp_strength;
        """,

        """
        -- Add season column
        ALTER TABLE tmp_stats ADD COLUMN season SMALLINT;
        
        UPDATE tmp_stats
        SET
            season = CASE 
                          WHEN date BETWEEN '2007-09-01' AND '2008-07-01' THEN 2007
                          WHEN date BETWEEN '2008-09-01' AND '2009-07-01' THEN 2008
                          WHEN date BETWEEN '2009-09-01' AND '2010-07-01' THEN 2009
                          WHEN date BETWEEN '2010-09-01' AND '2011-07-01' THEN 2010
                          WHEN date BETWEEN '2011-09-01' AND '2012-07-01' THEN 2011
                          WHEN date BETWEEN '2012-09-01' AND '2013-07-01' THEN 2012
                          WHEN date BETWEEN '2013-09-01' AND '2014-07-01' THEN 2013
                          WHEN date BETWEEN '2014-09-01' AND '2015-07-01' THEN 2014
                          WHEN date BETWEEN '2015-09-01' AND '2016-07-01' THEN 2015
                          WHEN date BETWEEN '2016-09-01' AND '2017-07-01' THEN 2016
                          WHEN date BETWEEN '2017-09-01' AND '2018-07-01' THEN 2017
                          WHEN date BETWEEN '2018-09-01' AND '2019-07-01' THEN 2018
                          WHEN date BETWEEN '2019-09-01' AND '2020-07-01' THEN 2019
                          WHEN date BETWEEN '2020-09-01' AND '2021-07-01' THEN 2020
                    END;
        """

        """
        DROP TABLE IF EXISTS goalie_stats;
        CREATE TABLE goalie_stats
        AS
        SELECT 
        MAX(player) as player,
        player_id,
        game_id,
        max(season) as season,
        max(date) as date,
        max(team) as team,
        max(opponent) as opponent,
        max(home) as home,
        strength,
        COALESCE(sum(Shots_a),0) Shots_a,
        COALESCE(sum(Goals_a),0) Goals_a,
        COALESCE(sum(fenwick_a),0) fenwick_a,
        COALESCE(sum(xg_a),0) xg_a,
        COALESCE(sum(corsi_a),0) corsi_a,
        max(toi_on) toi_on,
        max(toi_off) toi_off,
        COALESCE(sum(Shots_a_sa),0) Shots_a_sa,
        COALESCE(sum(Fenwick_a_sa),0) Fenwick_a_sa,
        COALESCE(sum(Corsi_a_sa),0) Corsi_a_sa,
        if_empty,
        CONCAT (player_id,'-', date, '-', strength, '-', if_empty) as primary_key,
        COALESCE(sum(shooter_xg_a),0) shooter_xg_a
        FROM tmp_stats 
        GROUP BY player_id, game_id, date, strength, if_empty
        """,


        """
        -- Get rid of duplicate rows .... I have no idea what's causing it
        DELETE FROM goalie_stats USING goalie_stats gs2
            WHERE 
                (goalie_stats.primary_key = gs2.primary_key AND goalie_stats.ctid < gs2.ctid)
                OR goalie_stats.opponent IS NULL;;
        """

    )

    for command in commands:
        cur.execute(command)

    # Commit All Changes
    conn.commit()


def aggregate_teams(cur, conn):
    """
    Aggregate Stats for Teams
    
    """
    commands = (
        """ 
        DROP TABLE IF EXISTS home;
        CREATE TABLE home
        AS
        SELECT 
        home_team as team,
        game_id,
        max(date) as date,
        max(away_team) as opponent,
        max(home_team) as home,
        strength,
        score_diff,
        sum(case when event IN ('GOAL', 'SHOT') AND ev_team = home_team then 1 else 0 end) Shots_f,
        sum(case when event = 'GOAL' AND ev_team = home_team then 1 else 0 end) Goals_f,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then 1 else 0 end) Fenwick_f,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then xg else 0 end) xg_f,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then shooter_xg else 0 end) shooter_xg_f,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS', 'BLOCK') AND ev_team = home_team then 1 else 0 end) Corsi_f,
        sum(case when event = 'PENL' AND ev_team = home_team then 1 else 0 end) Pent,
        sum(case when event = 'GIVE' AND ev_team = home_team then 1 else 0 end) Gives,
        sum(case when event = 'TAKE' AND ev_team = home_team then 1 else 0 end) Takes,
        sum(case when event = 'HIT' AND ev_team = home_team then 1 else 0 end) Hits_f,
        sum(case when event = 'FAC' AND ev_team = home_team then 1 else 0 end) Face_w,
        sum(case when event IN ('GOAL', 'SHOT') AND ev_team = away_team then 1 else 0 end) Shots_a,
        sum(case when event = 'GOAL' AND ev_team = away_team then 1 else 0 end) Goals_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then 1 else 0 end) Fenwick_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then xg else 0 end) xg_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then shooter_xg else 0 end) shooter_xg_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS', 'BLOCK') AND ev_team = away_team then 1 else 0 end) Corsi_a,
        sum(case when event = 'PENL' AND ev_team = away_team then 1 else 0 end) Pend,
        sum(case when event = 'HIT' AND ev_team = away_team then 1 else 0 end) Hits_a,
        sum(case when event = 'FAC' AND ev_team = away_team then 1 else 0 end) Face_l,
        sum(case when event = 'FAC' and home_zone = 'Off' then 1 else 0 end) Face_Off,
        sum(case when event = 'FAC' and home_zone = 'Def' then 1 else 0 end) Face_Def,
        sum(case when event = 'FAC' and home_zone = 'Neu' then 1 else 0 end) Face_Neu,
        if_empty
        FROM pbp 
        WHERE period != 5
        GROUP BY home_team, game_id, strength, score_diff, if_empty;
        """,

        """ 
        DROP TABLE IF EXISTS away;
        CREATE TABLE away
        AS
        SELECT 
        away_team as team,
        game_id,
        max(date) as date,
        max(home_team) as opponent,
        max(home_team) as home,
        strength,
        score_diff,
        sum(case when event IN ('GOAL', 'SHOT') AND ev_team = away_team then 1 else 0 end) Shots_f,
        sum(case when event = 'GOAL' AND ev_team = away_team then 1 else 0 end) Goals_f,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then 1 else 0 end) Fenwick_f,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then xg else 0 end) xg_f,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then shooter_xg else 0 end) shooter_xg_f,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS', 'BLOCK') AND ev_team = away_team then 1 else 0 end) Corsi_f,
        sum(case when event = 'PENL' AND ev_team = away_team then 1 else 0 end) Pent,
        sum(case when event = 'GIVE' AND ev_team = away_team then 1 else 0 end) Gives,
        sum(case when event = 'TAKE' AND ev_team = away_team then 1 else 0 end) Takes,
        sum(case when event = 'HIT' AND ev_team = away_team then 1 else 0 end) Hits_f,
        sum(case when event = 'FAC' AND ev_team = away_team then 1 else 0 end) Face_w,
        sum(case when event IN ('GOAL', 'SHOT') AND ev_team = home_team then 1 else 0 end) Shots_a,
        sum(case when event = 'GOAL' AND ev_team = home_team then 1 else 0 end) Goals_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then 1 else 0 end) Fenwick_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then xg else 0 end) xg_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then shooter_xg else 0 end) shooter_xg_a,
        sum(case when event IN ('GOAL', 'SHOT', 'MISS', 'BLOCK') AND ev_team = home_team then 1 else 0 end) Corsi_a,
        sum(case when event = 'PENL' AND ev_team = home_team then 1 else 0 end) Pend,
        sum(case when event = 'HIT' AND ev_team = home_team then 1 else 0 end) Hits_a,
        sum(case when event = 'FAC' AND ev_team = home_team then 1 else 0 end) Face_l,
        sum(case when event = 'FAC' and home_zone = 'Def' then 1 else 0 end) Face_Off,
        sum(case when event = 'FAC' and home_zone = 'Off' then 1 else 0 end) Face_Def,
        sum(case when event = 'FAC' and home_zone = 'Neu' then 1 else 0 end) Face_Neu,
        if_empty
        FROM pbp 
        WHERE period != 5
        GROUP BY away_team, game_id, strength, score_diff, if_empty;
        """,

        """
        -- Strength in pbp is 'home_num x away_num'...would need to flip this in away
        -- to get them on the same scale
        UPDATE away
        SET strength = SUBSTR(strength, 3, 1) || 'x' || SUBSTR(strength, 1, 1);
        """,

        """
        DROP TABLE IF EXISTS tmp_stats;
        CREATE TEMP TABLE tmp_stats
        AS
        SELECT * FROM home 
        UNION
        SELECT * FROM away;
        """,

        """
        DROP TABLE home;
        DROP TABLE away;
        """,

        """
        -- Mark if home or away team and make tmp_strength
        ALTER TABLE tmp_stats ADD COLUMN home_or_away TEXT, ADD COLUMN tmp_strength TEXT;
        
        UPDATE tmp_stats
        SET 
            home_or_away = CASE WHEN team = home THEN 'home' ELSE 'away' END,
            tmp_strength = CASE WHEN strength = '5x5' THEN '5v5' ELSE 'All' END;
        """,

        """
        -- Delete where team = null
        DELETE from tmp_stats
        WHERE
        team IS NULL;
        """,

        """
        ALTER TABLE tmp_stats ADD COLUMN toi BIGINT;
        
        UPDATE tmp_stats
            SET toi = team_toi.toi
        FROM team_toi
        WHERE
        team_toi.team = tmp_stats.team
        AND team_toi.game_id = tmp_stats.game_id
        AND team_toi.date = tmp_stats.date
        AND team_toi.strength = tmp_stats.strength
        AND team_toi.if_empty = tmp_stats.if_empty;
        """,

        """
        -- Add home team and opponent to TOI b4 adding it into master
        ALTER TABLE team_toi ADD COLUMN home TEXT, ADD COLUMN opponent TEXT;
        
        UPDATE team_toi p
        SET 
            home = t.home,
            opponent = t.opponent
        FROM tmp_stats t
        WHERE
        p.team = t.team
        AND p.date = t.date;
        """,

        """
        -- Insert those with '2x2' (NHL fucked up on shifts so undefined strength) into table

        INSERT INTO tmp_stats (team, game_id, date, home, opponent, strength, if_empty, toi)
        SELECT 
        p.team, p.game_id, p.date, p.home, p.opponent, p.strength, p.if_empty, p.toi
        FROM
        team_toi p
        WHERE NOT EXISTS (
            SELECT * from tmp_stats t 
            WHERE t.team = p.team
            AND t.date = p.date
            AND t.strength = p.strength
            AND t.if_empty = p.if_empty
        )
        AND
        p.toi> 0;
        """,

        """
        -- Get rid useless rows
        DELETE FROM tmp_stats
        WHERE toi IS NULL AND corsi_f = 0 AND corsi_a = 0;
        """,

        """
        ALTER TABLE tmp_stats 
        ADD COLUMN shots_f_sa REAL,
        ADD COLUMN fenwick_f_sa REAL,
        ADD COLUMN corsi_f_sa REAL,
        ADD COLUMN shots_a_sa REAL,
        ADD COLUMN fenwick_a_sa REAL,
        ADD COLUMN corsi_a_sa REAL;
        """,

        """
        UPDATE tmp_stats
        SET
        shots_f_sa = shots_f * (SELECT shots FROM score_coefficients WHERE tmp_stats.home_or_away = team 
                                AND tmp_stats.score_diff = score_coefficients.scorediff),
        fenwick_f_sa = fenwick_f * (SELECT fenwick FROM score_coefficients WHERE tmp_stats.home_or_away = team 
                                    AND tmp_stats.score_diff = score_coefficients.scorediff),
        corsi_f_sa = corsi_f * (SELECT corsi FROM score_coefficients WHERE tmp_stats.home_or_away = team 
                                AND tmp_stats.score_diff = score_coefficients.scorediff),
        
        shots_a_sa = shots_a * (SELECT shots FROM score_coefficients WHERE tmp_stats.home_or_away != team 
                                AND tmp_stats.score_diff = score_coefficients.scorediff),
        fenwick_a_sa = fenwick_a * (SELECT fenwick FROM score_coefficients WHERE tmp_stats.home_or_away != team 
                                    AND tmp_stats.score_diff = score_coefficients.scorediff),
        corsi_a_sa = corsi_a * (SELECT corsi FROM score_coefficients WHERE tmp_stats.home_or_away != team 
                                AND tmp_stats.score_diff = score_coefficients.scorediff);
        """,

        """
        -- Get rid of useless columns
        ALTER TABLE tmp_stats DROP COLUMN home_or_away, DROP COLUMN tmp_strength;
        """,

        """
        -- Add season column
        ALTER TABLE tmp_stats ADD COLUMN season SMALLINT;

        UPDATE tmp_stats
        SET
            season = CASE 
                          WHEN date BETWEEN '2007-09-01' AND '2008-07-01' THEN 2007
                          WHEN date BETWEEN '2008-09-01' AND '2009-07-01' THEN 2008
                          WHEN date BETWEEN '2009-09-01' AND '2010-07-01' THEN 2009
                          WHEN date BETWEEN '2010-09-01' AND '2011-07-01' THEN 2010
                          WHEN date BETWEEN '2011-09-01' AND '2012-07-01' THEN 2011
                          WHEN date BETWEEN '2012-09-01' AND '2013-07-01' THEN 2012
                          WHEN date BETWEEN '2013-09-01' AND '2014-07-01' THEN 2013
                          WHEN date BETWEEN '2014-09-01' AND '2015-07-01' THEN 2014
                          WHEN date BETWEEN '2015-09-01' AND '2016-07-01' THEN 2015
                          WHEN date BETWEEN '2016-09-01' AND '2017-07-01' THEN 2016
                          WHEN date BETWEEN '2017-09-01' AND '2018-07-01' THEN 2017
                          WHEN date BETWEEN '2018-09-01' AND '2019-07-01' THEN 2018
                          WHEN date BETWEEN '2019-09-01' AND '2020-07-01' THEN 2019
                          WHEN date BETWEEN '2020-09-01' AND '2021-07-01' THEN 2020
                    END;
        """,

        """
        DROP TABLE IF EXISTS team_stats;

        CREATE TABLE team_stats
        AS
        SELECT 
        team,
        game_id,
        max(season) as season,
        max(date) as date,
        max(opponent) as opponent,
        max(home) as home,
        strength,
        max(toi) toi,
        COALESCE(sum(Shots_f),0) Shots_f,
        COALESCE(sum(Goals_f),0) Goals_f,
        COALESCE(sum(Fenwick_f),0) Fenwick_f,
        COALESCE(sum(xg_f),0) xg_f,
        COALESCE(sum(Corsi_f),0) Corsi_f,
        COALESCE(sum(Shots_a),0) Shots_a,
        COALESCE(sum(Goals_a),0) Goals_a,
        COALESCE(sum(Fenwick_a),0) Fenwick_a,
        COALESCE(sum(xg_a),0) xg_a,
        COALESCE(sum(Corsi_a),0) Corsi_a,
        COALESCE(sum(Pent),0) Pent,
        COALESCE(sum(Pend),0) Pend,
        COALESCE(sum(Gives),0) Gives,
        COALESCE(sum(Takes),0) Takes,
        COALESCE(sum(Hits_f),0) Hits_f,
        COALESCE(sum(Hits_a),0) Hits_a,
        COALESCE(sum(Face_w),0) Face_w,
        COALESCE(sum(Face_l),0) Face_l,
        COALESCE(sum(Face_Off),0) Face_Off,
        COALESCE(sum(Face_Def),0) Face_Def,
        COALESCE(sum(Face_Neu),0) Face_Neu,
        COALESCE(sum(Shots_f_sa),0) Shots_f_sa,
        COALESCE(sum(Fenwick_f_sa),0) Fenwick_f_sa,
        COALESCE(sum(Corsi_f_sa),0) Corsi_f_sa,
        COALESCE(sum(Shots_a_sa),0) Shots_a_sa,
        COALESCE(sum(Fenwick_a_sa),0) Fenwick_a_sa,
        COALESCE(sum(Corsi_a_sa),0) Corsi_a_sa,
        if_empty,
        CONCAT (team,'-', date, '-', strength, '-', if_empty) as primary_key,
        COALESCE(sum(shooter_xg_a),0) shooter_xg_a,
        COALESCE(sum(shooter_xg_f),0) shooter_xg_f
        FROM tmp_stats
        GROUP BY team, game_id, date, strength, if_empty;
        """,

        """
        -- Get rid of duplicate rows .... I have no idea what's causing it
        DELETE FROM team_stats USING team_stats ts2
            WHERE team_stats.primary_key = ts2.primary_key AND team_stats.ctid < ts2.ctid;
        """
    )

    for command in commands:
        cur.execute(command)

    conn.commit()


def aggregate_skaters(cur, conn):
    """
    Aggregate numbers for skaters
    There's a shitload of comments throughout so look through it
    """

    ########################################
    # 1
    # Create Initial Tables for on-ice stats and individual stats
    ########################################

    # Create the final table first
    cur.execute(
        """
        DROP TABLE IF EXISTS tmp_stats;
        
        CREATE TEMP TABLE tmp_stats(
        player TEXT,
        player_id BIGINT,
        game_id SMALLINT,
        date TEXT,
        team TEXT,
        opponent TEXT,
        home TEXT,
        strength TEXT,
        score_diff SMALLINT,
        goals SMALLINT,
        a1 SMALLINT,
        a2 SMALLINT,
        isf SMALLINT,
        ifen SMALLINT,
        ixg REAL, 
        shooter_ixg REAL,
        icors SMALLINT,
        iBlocks SMALLINT,
        pen_drawn SMALLINT,
        pen_taken SMALLINT,
        give SMALLINT,
        take SMALLINT,
        hit_for SMALLINT,
        hit_against SMALLINT,
        ifac_win SMALLINT,
        ifac_loss SMALLINT,
        shots_f SMALLINT,
        goals_f SMALLINT,
        fenwick_f SMALLINT,
        xg_f REAL, 
        shooter_xg_f REAL,
        corsi_f SMALLINT,
        shots_a SMALLINT,
        goals_a SMALLINT,
        fenwick_a SMALLINT,
        xg_a REAL, 
        shooter_xg_a REAL,
        corsi_a SMALLINT,
        face_off SMALLINT,
        face_def SMALLINT,
        face_neu SMALLINT,
        if_empty SMALLINT,
        toi_on BIGINT, 
        toi_off BIGINT,
        position TEXT,
        handedness TEXT,
        shots_f_off SMALLINT,
        goals_f_off SMALLINT,
        fenwick_f_off SMALLINT,
        xg_f_off REAL, 
        shooter_xg_f_off REAL, 
        corsi_f_off SMALLINT,
        shots_a_off SMALLINT,
        goals_a_off SMALLINT,
        fenwick_a_off SMALLINT,
        xg_a_off REAL, 
        shooter_xg_a_off REAL, 
        corsi_a_off SMALLINT,
        face_off_off SMALLINT,
        face_neu_off SMALLINT,
        face_def_off SMALLINT,
        shots_f_sa REAL,
        fenwick_f_sa REAL,
        corsi_f_sa REAL,
        shots_a_sa REAL,
        fenwick_a_sa REAL,
        corsi_a_sa REAL, 
        shots_f_off_sa REAL,
        fenwick_f_off_sa REAL,
        corsi_f_off_sa REAL,
        shots_a_off_sa REAL,
        fenwick_a_off_sa REAL,
        corsi_a_off_sa REAL
        );
        """
    )

    # Drop if exists and create framework for individual stats Table
    cur.execute(
        """
        DROP TABLE IF EXISTS ind_skater_stats_tmp;
        
        CREATE TEMP TABLE ind_skater_stats_tmp(
        player TEXT,
        player_id BIGINT,
        game_id SMALLINT,
        date TEXT,
        team TEXT,
        opponent TEXT,
        home TEXT,
        strength TEXT,
        score_diff SMALLINT,
        goals SMALLINT,
        a1 SMALLINT,
        a2 SMALLINT,
        isf SMALLINT,
        ifen SMALLINT,
        ixg REAL, 
        shooter_ixg REAL,
        icors SMALLINT,
        iBlocks SMALLINT,
        pen_drawn SMALLINT,
        pen_taken SMALLINT,
        give SMALLINT,
        take SMALLINT,
        hit_for SMALLINT,
        hit_against SMALLINT,
        ifac_win SMALLINT,
        ifac_loss SMALLINT,
        if_empty SMALLINT
        );
        """
    )

    #################################################
    # 2
    # Aggregate individual numbers for each event player 1-3
    # Then group up by player, game, strength, if_empty (since bec. 3 slots can have multiple entries for each type
    # Don't group by score_diff bec. not needed (don't need to score adjust)
    #################################################

    for x in range(1, 4):
        players = {"name": ''.join(['p', str(x), "_name"]), "id": ''.join(['p', str(x), '_id'])}

        cur.execute(
            """
            INSERT INTO ind_skater_stats_tmp 
            SELECT 
            max({0}) as player,
            {1} as player_id,
            game_id,
            date,
            max(ev_team) as team,
            max(case when ev_team = home_team then away_team else home_team end) as opponent,
            max(home_team) as home,
            strength,
            score_diff,
            sum(case when event = 'GOAL' AND p1_id = {1} then 1 else 0 end) goals,
            sum(case when event = 'GOAL' AND p2_id = {1} then 1 else 0 end) a1,
            sum(case when event = 'GOAL' AND p3_id = {1} then 1 else 0 end) a2,
            sum(case when event IN ('GOAL', 'SHOT') AND p1_id = {1} then 1 else 0 end) iSf,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND p1_id = {1} then 1 else 0 end) iFen,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND p1_id = {1} then xg else 0 end) ixg,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND p1_id = {1} then shooter_xg else 0 end) shooter_ixg,
            sum(case when (event IN ('GOAL', 'SHOT', 'MISS') AND p1_id = {1}) OR (event = 'BLOCK' AND p2_id = {1}) then 1 else 0 end) iCors,
            sum(case when event = 'BLOCK' AND p1_id = {1} then 1 else 0 end) iBlock, 
            sum(case when event = 'PENL' AND p2_id = {1} then 1 else 0 end) pen_drawn,
            sum(case when event = 'PENL' AND p1_id = {1} then 1 else 0 end) pen_taken,
            sum(case when event = 'GIVE' AND p1_id = {1} then 1 else 0 end) give,
            sum(case when event = 'TAKE' AND p1_id = {1} then 1 else 0 end) take,
            sum(case when event = 'HIT' AND p1_id = {1} then 1 else 0 end) hit_for,
            sum(case when event = 'HIT' AND p2_id = {1} then 1 else 0 end) hit_against,
            sum(case when event = 'FAC' AND p1_id = {1} then 1 else 0 end) ifac_win,
            sum(case when event = 'FAC' AND p2_id = {1} then 1 else 0 end) ifac_loss,
            if_empty
            FROM pbp 
            GROUP BY {1}, game_id, date, strength, score_diff, if_empty;
            """.format(players["name"], players["id"]),
        )

    # Fix Strength if Away Team
    cur.execute(
        """
        UPDATE ind_skater_stats_tmp
            SET strength = CASE WHEN team != home 
                                THEN SUBSTR(strength, 3, 1) || 'x' || SUBSTR(strength, 1, 1)
                                ELSE strength 
                           END;
        """
    )

    cur.execute(
        """
        DROP TABLE IF EXISTS ind_skater_stats;
        CREATE TEMP TABLE ind_skater_stats
        AS
        SELECT 
        MAX(player) as player,
        player_id,
        game_id,
        date,
        MAX(team) as team,
        MAX(opponent) as opponent,
        MAX(home) as home,
        strength,
        COALESCE(SUM(goals),0) goals,
        COALESCE(SUM(a1),0) a1,
        COALESCE(SUM(a2),0) a2,
        COALESCE(SUM(isf),0) isf,
        COALESCE(SUM(ifen),0) ifen,
        COALESCE(SUM(ixg),0) ixg,
        COALESCE(SUM(ixg),0) shooter_ixg,
        COALESCE(SUM(icors),0) icors,
        COALESCE(SUM(iBlocks),0) iBlocks,
        COALESCE(SUM(pen_drawn),0) pen_drawn,
        COALESCE(SUM(pen_taken),0) pen_taken,
        COALESCE(SUM(give),0) gives,
        COALESCE(SUM(take),0) takes,
        COALESCE(SUM(hit_for),0) hit_for,
        COALESCE(SUM(hit_against),0) hit_against,
        COALESCE(SUM(ifac_win),0) ifac_win,
        COALESCE(SUM(ifac_loss),0) ifac_loss,
        if_empty
        FROM ind_skater_stats_tmp
        GROUP BY player_id, game_id, date, strength, if_empty;
        
        DROP TABLE IF EXISTS ind_skater_stats_tmp;
        """
    )

    #################################################
    # 3
    # Aggregate on ice numbers for each home and away play (1-6)
    # Then group up by player, game, strength, if_empty (since bec. 3 slots can have multiple for each
    # Don't group by score_diff bec. not needed (don't need to score adjust)
    #################################################

    for x in range(1, 7):
        players = [''.join(['homeplayer', str(x)]),
                   ''.join(['homeplayer', str(x), '_id']),
                   ''.join(['awayplayer', str(x)]),
                   ''.join(['awayplayer', str(x), '_id'])
                   ]

        commands = (

            """
            DROP TABLE IF EXISTS home;
            CREATE TABLE home
            AS
            SELECT 
            max({0}) as player,
            {1} as player_id,
            game_id,
            max(date) as date,
            max(home_team) as team,
            max(away_team) as opponent,
            max(home_team) as home,
            strength,
            score_diff,
            sum(case when event IN ('SHOT', 'GOAL') AND ev_team = home_team then 1 else 0 end) Shots_f,
            sum(case when event = 'GOAL' AND ev_team = home_team then 1 else 0 end) Goals_f,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then 1 else 0 end) Fenwick_f,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then xg else 0 end) xg_f,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then shooter_xg else 0 end) shooter_xg_f,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS', 'BLOCK') AND ev_team = home_team then 1 else 0 end) Corsi_f,
            sum(case when event IN ('SHOT', 'GOAL') AND ev_team = away_team then 1 else 0 end) Shots_a,
            sum(case when event = 'GOAL' AND ev_team = away_team then 1 else 0 end) Goals_a,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then 1 else 0 end) Fenwick_a,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then xg else 0 end) xg_a,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then shooter_xg else 0 end) shooter_xg_a,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS', 'BLOCK') AND ev_team = away_team then 1 else 0 end) Corsi_a,
            sum(case when event = 'FAC' and home_zone = 'Off' then 1 else 0 end) Face_Off,
            sum(case when event = 'FAC' and home_zone = 'Def' then 1 else 0 end) Face_Def,
            sum(case when event = 'FAC' and ev_zone = 'Neu' then 1 else 0 end) Face_Neu,
            if_empty
            FROM pbp 
            GROUP BY {1}, game_id, strength, score_diff, if_empty;
            """.format(players[0], players[1]),

            """ 
            DROP TABLE IF EXISTS away;
            CREATE TABLE away
            AS
            SELECT 
            max({0}) as player,
            {1} as player_id,
            game_id,
            max(date) as date,
            max(away_team) as team,
            max(home_team) as opponent,
            max(home_team) as home,
            strength,
            score_diff,
            sum(case when event IN ('SHOT', 'GOAL') AND ev_team = away_team then 1 else 0 end) Shots_f,
            sum(case when event = 'GOAL' AND ev_team = away_team then 1 else 0 end) Goals_f,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then 1 else 0 end) Fenwick_f,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then xg else 0 end) xg_f,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = away_team then shooter_xg else 0 end) shooter_xg_f,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS', 'BLOCK') AND ev_team = away_team then 1 else 0 end) Corsi_f,
            sum(case when event IN ('SHOT', 'GOAL') AND ev_team = home_team then 1 else 0 end) Shots_a,
            sum(case when event = 'GOAL' AND ev_team = home_team then 1 else 0 end) Goals_a,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then 1 else 0 end) Fenwick_a,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then xg else 0 end) xg_a,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS') AND ev_team = home_team then shooter_xg else 0 end) shooter_xg_a,
            sum(case when event IN ('GOAL', 'SHOT', 'MISS', 'BLOCK') AND ev_team = home_team then 1 else 0 end) Corsi_a,
            sum(case when event = 'FAC' and home_zone = 'Def' then 1 else 0 end) Face_Off,
            sum(case when event = 'FAC' and home_zone = 'Off' then 1 else 0 end) Face_Def,
            sum(case when event = 'FAC' and ev_zone = 'Neu' then 1 else 0 end) Face_Neu,
            if_empty
            FROM pbp 
            GROUP BY {1}, game_id, strength, score_diff, if_empty;
            """.format(players[2], players[3]),

            """
            -- Strength in pbp is 'home_num x away_num'...would need to flip this in away to get them on the same scale
            UPDATE away
            SET strength = SUBSTR(strength, 3, 1) || 'x' || SUBSTR(strength, 1, 1);
            """,

            """
            INSERT INTO tmp_stats(player, player_id, game_id, date, team, opponent, home, strength, score_diff, Shots_f,
            Goals_f, Fenwick_f, xg_f, shooter_xg_f, Corsi_f, Shots_a, Goals_a, Fenwick_a, xg_a, shooter_xg_a, Corsi_a,
            Face_Off, Face_Def, Face_Neu, if_empty)
            SELECT 
            player, player_id, game_id, date, team, opponent, home, strength, score_diff, Shots_f,
            Goals_f, Fenwick_f, xg_f, shooter_xg_f, Corsi_f, Shots_a, Goals_a, Fenwick_a, xg_a, shooter_xg_a, Corsi_a,
            Face_Off, Face_Def, Face_Neu, if_empty
            FROM home;
            
            INSERT INTO tmp_stats(player, player_id, game_id, date, team, opponent, home, strength, score_diff, Shots_f,
            Goals_f, Fenwick_f, xg_f, shooter_xg_f, Corsi_f, Shots_a, Goals_a, Fenwick_a, xg_a, shooter_xg_a, Corsi_a,
            Face_Off, Face_Def, Face_Neu, if_empty)
            SELECT 
            player, player_id, game_id, date, team, opponent, home, strength, score_diff, Shots_f,
            Goals_f, Fenwick_f, xg_f, shooter_xg_f, Corsi_f, Shots_a, Goals_a, Fenwick_a, xg_a, shooter_xg_a, Corsi_a,
            Face_Off, Face_Def, Face_Neu, if_empty
            FROM away;
            """,

            """
            DROP TABLE home;
            DROP TABLE away;
            """
        )

        for command in commands:
            cur.execute(command)

    #################################################
    # 4
    # Main portion of code
    # Adds in: TOI (on and off), score adjusted numbers (on and off), off-ice numbers, and season
    # Also merges in individual numbers to master
    # In process gets rid of useless rows and stuff
    #################################################

    cur.execute(
        """
        -- Delete where player or player_id = null
        DELETE from tmp_stats
        WHERE
        player IS NULL OR player_id IS NULL;
        
        
        -- Problem with this is that it gives TOI to each score diff
        -- I just select MAX later when group by
        UPDATE tmp_stats
        SET toi_on = player_toi.toi_on,
            toi_off = player_toi.toi_off
        FROM player_toi
        WHERE
        player_toi.player_id = tmp_stats.player_id
        AND player_toi.game_id  = tmp_stats.game_id
        AND player_toi.strength = tmp_stats.strength
        AND player_toi.if_empty = tmp_stats.if_empty;
        
        
        -- Add home team and opponent to TOI b4 adding it into master
        UPDATE player_toi p
        SET 
            home = t.home,
            opponent = t.opponent
        FROM tmp_stats t
        WHERE
        p.player_id = t.player_id  
        AND p.date = t.date;


        -- Insert those with '2x2' (NHL fucked up on shifts so undefined strength) into table
        INSERT INTO tmp_stats (player, player_id, game_id, date, team, home, opponent, strength, if_empty, toi_on, toi_off)
        SELECT
        p.player, p.player_id, p.game_id, p.date, p.team, p.home, p.opponent, p.strength, p.if_empty, p.toi_on, p.toi_off
        FROM
        player_toi p
        WHERE NOT EXISTS (
            SELECT * from tmp_stats t 
            WHERE t.player_id = p.player_id
            AND t.date = p.date
            AND t.strength = p.strength
            AND t.if_empty = p.if_empty
        )
        AND
        p.position != 'G'
        AND
        p.toi_on > 0;
        
        
        -- Add Position and handedness
        UPDATE tmp_stats
        SET 
            position = nhl_players.position,
            handedness = nhl_players.shoots_catches
        FROM nhl_players
        WHERE
        nhl_players.id = tmp_stats.player_id;
        
        
        -- Get rid of useless rows 
        DELETE FROM tmp_stats
        WHERE (toi_on IS NULL AND toi_off IS NULL AND iCors = 0) OR position = 'G';
        
        
        -- Mark if home or away team and make tmp_strength
       ALTER TABLE tmp_stats ADD COLUMN home_or_away TEXT, ADD COLUMN tmp_strength TEXT;
       UPDATE tmp_stats
        SET 
           home_or_away = CASE WHEN team = home THEN 'home' ELSE 'away' END,
           tmp_strength = CASE WHEN strength = '5x5' THEN '5v5' ELSE 'All' END;
           
           
       UPDATE tmp_stats
        SET
        shots_f_sa = shots_f * (SELECT shots FROM score_coefficients WHERE tmp_stats.home_or_away = team 
                                AND tmp_stats.score_diff = score_coefficients.scorediff),
        fenwick_f_sa = fenwick_f * (SELECT fenwick FROM score_coefficients WHERE tmp_stats.home_or_away = team 
                                    AND tmp_stats.score_diff = score_coefficients.scorediff),
        corsi_f_sa = corsi_f * (SELECT corsi FROM score_coefficients WHERE tmp_stats.home_or_away = team 
                                AND tmp_stats.score_diff = score_coefficients.scorediff),
        shots_a_sa = shots_a * (SELECT shots FROM score_coefficients WHERE tmp_stats.home_or_away != team 
                                AND tmp_stats.score_diff = score_coefficients.scorediff),
        fenwick_a_sa = fenwick_a * (SELECT fenwick FROM score_coefficients WHERE tmp_stats.home_or_away != team  
                                    AND tmp_stats.score_diff = score_coefficients.scorediff),
        corsi_a_sa = corsi_a * (SELECT corsi FROM score_coefficients WHERE tmp_stats.home_or_away != team 
                                AND tmp_stats.score_diff = score_coefficients.scorediff);
        
        
        -- Get rid of useless columns
        ALTER TABLE tmp_stats DROP COLUMN home_or_away, DROP COLUMN tmp_strength;
        
        
        -- Add season column
        ALTER TABLE tmp_stats ADD COLUMN season SMALLINT;

        UPDATE tmp_stats
        SET
            season = CASE 
                          WHEN date BETWEEN '2007-09-01' AND '2008-07-01' THEN 2007
                          WHEN date BETWEEN '2008-09-01' AND '2009-07-01' THEN 2008
                          WHEN date BETWEEN '2009-09-01' AND '2010-07-01' THEN 2009
                          WHEN date BETWEEN '2010-09-01' AND '2011-07-01' THEN 2010
                          WHEN date BETWEEN '2011-09-01' AND '2012-07-01' THEN 2011
                          WHEN date BETWEEN '2012-09-01' AND '2013-07-01' THEN 2012
                          WHEN date BETWEEN '2013-09-01' AND '2014-07-01' THEN 2013
                          WHEN date BETWEEN '2014-09-01' AND '2015-07-01' THEN 2014
                          WHEN date BETWEEN '2015-09-01' AND '2016-07-01' THEN 2015
                          WHEN date BETWEEN '2016-09-01' AND '2017-07-01' THEN 2016
                          WHEN date BETWEEN '2017-09-01' AND '2018-07-01' THEN 2017
                          WHEN date BETWEEN '2018-09-01' AND '2019-07-01' THEN 2018
                          WHEN date BETWEEN '2019-09-01' AND '2020-07-01' THEN 2019
                          WHEN date BETWEEN '2020-09-01' AND '2021-07-01' THEN 2020
                    END;
                     
                     
        DROP TABLE IF EXISTS skater_stats;
        CREATE TABLE skater_stats
        AS
        SELECT
        MAX(player) as player,
        player_id,
        MAX(position) as position,
        MAX(handedness) as handedness,
        max(season) as season,
        game_id,
        MAX(date) as date,
        MAX(team) as team,
        MAX(opponent) as opponent,
        MAX(home) as home,
        strength,
        MAX(toi_on) toi_on, 
        COALESCE(SUM(goals),0) goals,
        COALESCE(SUM(a1),0) a1,
        COALESCE(SUM(a2),0) a2,
        COALESCE(SUM(isf),0) isf,
        COALESCE(SUM(ifen),0) ifen,
        COALESCE(SUM(ixg),0) ixg,
        COALESCE(SUM(icors),0) icors,
        COALESCE(SUM(iBlocks),0) iBlocks,
        COALESCE(SUM(pen_drawn),0) pen_drawn,
        COALESCE(SUM(pen_taken),0) pen_taken,
        COALESCE(SUM(give),0) gives,
        COALESCE(SUM(take),0) takes,
        COALESCE(SUM(hit_for),0) hits_f,
        COALESCE(SUM(hit_against),0) hits_a,
        COALESCE(SUM(ifac_win),0) ifac_win,
        COALESCE(SUM(ifac_loss),0) ifac_loss,
        COALESCE(SUM(shots_f),0) shots_f,
        COALESCE(SUM(goals_f),0) goals_f,
        COALESCE(SUM(fenwick_f),0) fenwick_f,
        COALESCE(SUM(xg_f),0) xg_f,
        COALESCE(SUM(corsi_f),0) corsi_f,
        COALESCE(SUM(shots_a),0) shots_a,
        COALESCE(SUM(goals_a),0) goals_a,
        COALESCE(SUM(fenwick_a),0) fenwick_a,
        COALESCE(SUM(xg_a),0) xg_a,
        COALESCE(SUM(corsi_a),0) corsi_a,
        COALESCE(SUM(shots_f_sa),0) shots_f_sa,
        COALESCE(SUM(fenwick_f_sa),0) fenwick_f_sa,
        COALESCE(SUM(corsi_f_sa),0) corsi_f_sa,
        COALESCE(SUM(shots_a_sa),0) shots_a_sa,
        COALESCE(SUM(fenwick_a_sa),0) fenwick_a_sa,
        COALESCE(SUM(corsi_a_sa),0) corsi_a_sa, 
        COALESCE(SUM(face_off),0) face_off,
        COALESCE(SUM(face_def),0) face_def,
        COALESCE(SUM(face_neu),0) face_neu,
        COALESCE(MAX(toi_off),0) toi_off,
        SUM(shots_f_off) shots_f_off,
        SUM(goals_f_off) goals_f_off,
        SUM(fenwick_f_off) fenwick_f_off,
        SUM(xg_f_off) xg_f_off,
        SUM(corsi_f_off) corsi_f_off,
        SUM(shots_a_off) shots_a_off,
        SUM(goals_a_off) goals_a_off,
        SUM(fenwick_a_off) fenwick_a_off,
        SUM(xg_a_off) xg_a_off,
        SUM(corsi_a_off) corsi_a_off,
        SUM(shots_f_off_sa) shots_f_off_sa,
        SUM(fenwick_f_off_sa) fenwick_f_off_sa,
        SUM(corsi_f_off_sa) corsi_f_off_sa,
        SUM(shots_a_off_sa) shots_a_off_sa,
        SUM(fenwick_a_off_sa) fenwick_a_off_sa,
        SUM(corsi_a_off_sa) corsi_a_off_sa,
        SUM(face_off_off) face_off_off,
        SUM(face_neu_off) face_neu_off,
        SUM(face_def_off) face_def_off,
        if_empty,
        CONCAT (player_id,'-', date, '-', strength, '-', if_empty) as primary_key,
        COALESCE(SUM(shooter_xg_a),0) shooter_xg_a,
        SUM(shooter_xg_a_off) shooter_xg_a_off,
        COALESCE(SUM(shooter_xg_f),0) shooter_xg_f,
        SUM(shooter_xg_f_off) shooter_xg_f_off,
        COALESCE(SUM(shooter_ixg),0) shooter_ixg
        FROM tmp_stats
        GROUP BY player_id, game_id, date, strength, if_empty;
        
        
        UPDATE skater_stats
        SET
            shots_f_off = COALESCE(team_stats.shots_f - skater_stats.shots_f ,0),
            goals_f_off = COALESCE(team_stats.goals_f - skater_stats.goals_f ,0),
            fenwick_f_off = COALESCE(team_stats.fenwick_f - skater_stats.fenwick_f ,0),
            xg_f_off = COALESCE(team_stats.xg_f - skater_stats.xg_f, 0),
            shooter_xg_f_off = COALESCE(team_stats.shooter_xg_f - skater_stats.shooter_xg_f, 0),
            corsi_f_off = COALESCE(team_stats.corsi_f - skater_stats.corsi_f ,0),
            shots_a_off =  COALESCE(team_stats.shots_a - skater_stats.shots_a ,0),
            goals_a_off = COALESCE(team_stats.goals_a - skater_stats.goals_a ,0),
            fenwick_a_off = COALESCE(team_stats.fenwick_a - skater_stats.fenwick_a ,0),
            xg_a_off = COALESCE(team_stats.xg_a - skater_stats.xg_a, 0),
            shooter_xg_a_off = COALESCE(team_stats.shooter_xg_a - skater_stats.shooter_xg_a, 0),
            corsi_a_off = COALESCE(team_stats.corsi_a - skater_stats.corsi_a ,0),

            shots_f_off_sa = COALESCE(team_stats.shots_f_sa - skater_stats.shots_f_sa ,0),
            fenwick_f_off_sa = COALESCE(team_stats.fenwick_f_sa - skater_stats.fenwick_f_sa ,0),
            corsi_f_off_sa = COALESCE(team_stats.corsi_f_sa - skater_stats.corsi_f_sa ,0),
            shots_a_off_sa =  COALESCE(team_stats.shots_a_sa - skater_stats.shots_a_sa ,0),
            fenwick_a_off_sa = COALESCE(team_stats.fenwick_a_sa - skater_stats.fenwick_a_sa ,0),
            corsi_a_off_sa = COALESCE(team_stats.corsi_a_sa - skater_stats.corsi_a_sa ,0),

            face_off_off = COALESCE(team_stats.face_off - skater_stats.face_off ,0),
            face_def_off = COALESCE(team_stats.face_def - skater_stats.face_def ,0),
            face_neu_off = COALESCE(team_stats.face_neu - skater_stats.face_neu ,0)
        FROM team_stats
        WHERE
        skater_stats.team = team_stats.team
        AND skater_stats.game_id = team_stats.game_id
        AND skater_stats.strength = team_stats.strength
        AND skater_stats.if_empty = team_stats.if_empty;
        
        -- Merge the two above
        UPDATE skater_stats t
        SET 
            goals = i.goals,
            a1 = i.a1,
            a2 = i.a2,
            isf = i.isf,
            ifen = i.ifen,
            ixg = i.ixg, 
            shooter_ixg = i.shooter_ixg,
            icors = i.icors,
            iBlocks = i.iBlocks,
            pen_drawn = i.pen_drawn,
            pen_taken = i.pen_taken,
            gives = i.gives,
            takes = i.takes,
            hits_f = i.hit_for,
            hits_a = i.hit_against,
            ifac_win = i.ifac_win,
            ifac_loss = i.ifac_loss
        FROM ind_skater_stats i
        WHERE
            t.player_id = i.player_id
            AND t.game_id = i.game_id
            AND t.date = i.date
            AND t.strength = i.strength
            AND t.if_empty = i.if_empty;
            
            
        DROP TABLE ind_skater_stats;
                    

        -- Get rid of duplicate rows ... I have no idea what's causing it
        DELETE FROM skater_stats USING skater_stats ss2
            WHERE skater_stats.primary_key = ss2.primary_key AND skater_stats.ctid < ss2.ctid;
            
        """
    )

    # Commit All Changes
    conn.commit()


def aggregate_all():
    """
    Aggregate stats for each - Teams/Goalies/Skaters
    """

    try:
        conn = psycopg2.connect(host=HOST, database="nhl_data", user=USERNAME, password=PASSWORD)
        conn_web = psycopg2.connect(host=HOST, database=SITE_DB, user=USERNAME, password=PASSWORD)
        cur = conn.cursor()
        cur_web = conn_web.cursor()

        add_score_column(cur, conn)
        add_empty_net_column(cur, conn)
        # If the Strength is like 5x10 just change it to 5x5
        cur.execute("""UPDATE pbp SET strength = '5x5' WHERE char_length(strength) > 3;""")
        conn.commit()

        aggregate_teams(cur, conn)
        print('\nFinished aggregating teams')
        aggregate_goalies(cur, conn)
        print('Finished aggregating goalies')
        aggregate_skaters(cur, conn)
        print('Finished aggregating skaters')

        add_goalies_to_web(cur_web)
        add_teams_to_web(cur_web)
        add_skaters_to_web(cur_web)

        drop_tables(cur, conn)

        cur.close()
        conn.commit()
        cur_web.close()
        conn_web.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()





