import numpy as np
import re
import pandas as pd
import coords_adjs.rink_coords_adjustment as rca

pd.options.mode.chained_assignment = None  # default='warn' -> Stops it from giving me some error


def has_numbers(s):
    return bool(re.search(r'-?\d+', s))


def fix_df(df):
    """
    Fixes df so it can be processed for rink adjustments
    
    :param df: Full DataFrame
    
    :return: Fixed DataFrame
    """
    # Only take events we need
    pbp_df = df[df.event.isin(["SHOT", "GOAL", "MISS"])]

    pbp_df.xc = pd.to_numeric(pbp_df.xc, errors='coerce')
    pbp_df.yc = pd.to_numeric(pbp_df.yc, errors='coerce')

    # Add a 'Direction' column to indicate the primary direction for shots. The heuristic to determine
    # direction is the sign of the median of the X coordinate of shots in each period. This then lets us filter
    # out shots that originate from back in the defensive zone when the signs don't match
    gp_groups = pbp_df.groupby(by=['date', 'game_id', 'period'])['xc', 'yc']
    meanies = gp_groups.transform(np.median)  # will give us game/period median for X and Y for every data point
    pbp_df['Direction'] = np.sign(meanies['xc'])

    # FWIW, Cole Anderson doesn't use this
    #valid_shots = pbp_df[np.sign(pbp_df.xc) == pbp_df.Direction].copy()
    #valid_shots['xc'], valid_shots['yc'] = zip(*valid_shots.apply(lambda x: (x.xc, x.yc) if x.xc > 0 else (-x.xc, -x.yc), axis=1))

    # should actually write this to a CSV as up to here is the performance intensive part
    #pbp_df['xc'], pbp_df['yc'] = zip(*pbp_df.apply(lambda x: (x.xc, x.yc) if x.xc > 0 else (-x.xc, -x.yc), axis=1))
    pbp_df['xc'], pbp_df['yc'] = zip(*pbp_df.apply(lambda x: (x.xc, x.yc) if x.Direction > 0 else (-x.xc, -x.yc), axis=1))

    return pbp_df


def create_cdfs(shots_df, rink_adjuster):
    """
    Goes through and creates cdf for each team
    
    :param shots_df: df with only - Goals, SOG, and Misses
    :param rink_adjuster: RinkAdjust object
    
    :return: None
    """
    # Now rip through each team and create a CDF for that team and for the other 29 teams in the league
    # For each home rink
    for team in sorted(shots_df.home_team.unique()):
        # Split shots into team arena and all other rinks
        shot_data = shots_df.copy(deep=True)
        rink_shots = shot_data[shot_data.home_team == team]
        rest_of_league = shot_data[shot_data.home_team != team]

        # Create teamxcdf and otherxcdf for rink adjustment
        rink_adjuster.addTeam(team, rink_shots, rest_of_league)


def adjust_play(play, rink_adjuster):
    """
    Apply rink adjustments to play
    
    :param play: given play in game
    :param rink_adjuster: RinkAdjust object
    
    :return: newx, newy
    """
    if play['Event'] in ["SHOT", "GOAL", "MISS"]:
        if play['xC'] != '' and play['yC'] != '':
            # abs() for xC because all coordinates are made positive for cdf (to make it normal)
            newx, newy = rink_adjuster.rink_bias_adjust(abs(float(play['xC'])), float(play['yC']), play['Home_Team'])

            # if xC is really negative (because cdf only deals in positives - unlike yc) change it back
            newx = -newx if float(play['xC']) < 0 else newx
        else:
            # Why not...It needs to be given something
            newx, newy = 45, 0
    elif not has_numbers(str(play['xC'])) or not has_numbers(str(play['yC'])):
        newx, newy = play['xC'], play['yC']
    else:
        newx, newy = float(play['xC']), float(play['yC'])

    return newx, newy


def adjust_df(pbp_df, rink_adjuster):
    """
    Apply rink adjustments to PBP. Iterates through every play and adjusts from there
    
    :param pbp_df: PBP DataFrame
    :param rink_adjuster: RinkAdjust object
    
    :return: Adjusted DataFrame
    """
    df_dict = pbp_df.to_dict('records')

    pbp_df['xC_adj'], pbp_df['yC_adj'] = map(list, zip(*[adjust_play(row, rink_adjuster) for row in df_dict]))

    return pbp_df


def adjust(df):
    """
    Take a DataFrame and:
    1. Creates CDF's for each team
    2. Adjusts given games
    
    **Note: I advise not feeding this a DataFrame with less than one year's worth of data
    
    :param df: DataFrame of games
    
    :return: DataFrame with distance Rink Adjusted
    """
    rink_adjuster = rca.RinkAdjust()

    # Make sure coords columns aren't string
    df['xC'] = df['xC'].astype(float)
    df['yC'] = df['yC'].astype(float)

    create_cdfs(fix_df(df), rink_adjuster)
    pbp_df = adjust_df(df, rink_adjuster)

    return pbp_df


def main():
    pass

if __name__ == "__main__":
    main()







