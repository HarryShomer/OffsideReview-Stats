"""
Clean the data so we can feed it into the model to get the xG
"""

import pandas as pd
import numpy as np
from math import sqrt
import shared


def fix_prev_event(row):
    """
    More mistakes by the NHL. A Faceoff must come between each of these event so just input a faceoff from center ice
    That'll only be be "wrong" for ESITR (sometimes). 
    
    :param row: play
    
    :return: fixed row
    """
    if row['prev_event'] in ["PSTR", "GOAL", "EISTR"]:
        row['prev_event'] = "FAC"
        row['prev_xC_adj'] = 0
        row['prev_yC_adj'] = 0

    return row


def fix_score_cat(row):
    """
    When a goal was scored the I recorded the score with the goal which is obviously incorrect
    
    :param row: 
    
    :return: fixed score
    """
    if row['Event'] == "GOAL":
        if row['Ev_Team'] == row['Home_Team']:
            row["Home_Score"] -= 1
        else:
            row["Away_Score"] -= 1

    return row["Home_Score"], row["Away_Score"]


def if_empty_net(row):
    """
    Check if it's an empty net. 
    
    :param row: play in pbp
    
    :return: if non-event's team net is empty
    """
    if row['Ev_Team'] == row['Home_Team']:
        return 1 if row['Away_Goalie'] == "Empty" else 0
    else:
        return 1 if row['Home_Goalie'] == "Empty" else 0


def get_angle_change(row):
    """
    Calculate the angle change on a rebound shot
    
    :param row: play
    
    :return: angle change if rebound otherwise zero
    """
    if row['is_rebound'] == 0:
        return 0

    current_angle = 90-abs(row['Angle'])
    prev_angle = 90-abs(row['prev_angle'])

    if np.sign(row['Angle']) == np.sign(row['prev_angle']):
        return abs(current_angle-prev_angle)
    else:
        return abs(current_angle+prev_angle)


def get_prev_angle(row):
    """
    Get the angle for the previous shot (only for rebound)
    
    :param row: play 
    
    :return: angle for shot before rebounds
    """
    if row['prev_event'] == "SHOT" and row['Event'] == "SHOT":
        return 90 if row['prev_yC_adj'] == 0 else np.arctan((89.45 - abs(row['prev_xC_adj'])) / row['prev_yC_adj']) * (180/np.pi)
    else:
        return 0


def get_distance(row):
    try:
        return sqrt((row['xC_adj'] - row['prev_xC_adj']) ** 2 + (row['yC_adj'] - row['prev_yC_adj']) ** 2)
    except TypeError:
        # Debugging
        print(row["prev_event"], row['xC_adj'], row['prev_xC_adj'], row['yC_adj'], row['prev_yC_adj'])
        exit()


def fix_strength(row):
    """
    Fix the strength - flip if away team is event team
    
    :param row: play
    
    :return: "fixed" strength
    """
    if row['Ev_Team'] == row['Home_Team']:
        return row['Strength']
    else:
        return "x".join([row['Strength'][2], row['Strength'][0]])


def if_off_wing(row):
    """
    Check if the shot was taken by a player on their off wing
    1. Home Team: negative y axis is left side
    2. Away Team: positive y axis is left side
    
    :param row: given event
    
    :return: boolean - yes for off wing no for same
    """

    # L=Left, R=Right
    if row['Home_Team'] == row['Ev_Team']:
        direction = "L" if row['Period'] % 2 != 0 else "R"
    else:
        direction = "L" if row['Period'] % 2 == 0 else "R"

    if row['shooter_hand'] != direction:
        return 1 if row['yC_adj'] < 0 else 0
    else:
        return 0 if row['yC_adj'] < 0 else 1


def get_previous_event_info(df):
    """
    Get the info for the last event
    Note: The pbp is already sorted by game, period, and time...so don't need to worry about time
    
    :param df: DataFrame
    
    :return: df with stuff added
    """
    # Get All previous shit
    df['prev_event'] = df.groupby(['Game_Id', 'Period'])['Event'].shift(1)
    df['prev_ev_team'] = df.groupby(['Game_Id', 'Period'])['Ev_Team'].shift(1)
    df['prev_seconds'] = df.groupby(['Game_Id', 'Period'])['Seconds_Elapsed'].shift(1)
    df['time_elapsed'] = df['Seconds_Elapsed'] - df['prev_seconds']
    df['prev_ev_zone'] = df.groupby(['Game_Id', 'Period'])['Ev_Zone'].shift(1)
    df['prev_home_zone'] = df.groupby(['Game_Id', 'Period'])['Home_Zone'].shift(1)
    df['prev_xC_adj'] = df.groupby(['Game_Id', 'Period'])['xC_adj'].shift(1)
    df['prev_yC_adj'] = df.groupby(['Game_Id', 'Period'])['yC_adj'].shift(1)

    # Change giveaway to takeaway for other team
    df['prev_ev_team'] = np.where(df['prev_event'] != "GIVE", df["prev_ev_team"],
                                  np.where(df['prev_ev_team'] == df['Home_Team'], df["Away_Team"], df["Home_Team"]))
    df['prev_event'] = np.where(df['prev_event'] == "GIVE", "TAKE", df['prev_event'])

    # If last event was by event team
    df['if_prev_ev_team'] = np.where(df['Ev_Team'] == df['prev_ev_team'], 1, 0)

    # Get if last event was by event team for specified events
    df['prev_evTeam_Fac'] = np.where((df['if_prev_ev_team'] == 1) & (df['prev_event'] == "FAC"), 1, 0)
    df['prev_evTeam_NonSog'] = np.where((df['if_prev_ev_team'] == 1) & (df['prev_event'].isin(["MISS", "BLOCK"])), 1, 0)
    df['prev_evTeam_NonShot'] = np.where((df['if_prev_ev_team'] == 1) & (df['prev_event'].isin(["TAKE", "HIT"])), 1, 0)
    df['prev_evTeam_Sog'] = np.where((df['if_prev_ev_team'] == 1) & (df['prev_event'] == "SHOT"), 1, 0)

    # Get if last event was by non-event team for specified events
    df['prev_non_evTeam_Fac'] = np.where((df['if_prev_ev_team'] == 0) & (df['prev_event'] == "FAC"), 1, 0)
    df['prev_non_evTeam_NonSog'] = np.where((df['if_prev_ev_team'] == 0) & (df['prev_event'].isin(["MISS", "BLOCK"])), 1, 0)
    df['prev_non_evTeam_NonShot'] = np.where( (df['if_prev_ev_team'] == 0) & (df['prev_event'].isin(["TAKE", "HIT"])), 1, 0)
    df['prev_non_evTeam_Sog'] = np.where((df['if_prev_ev_team'] == 0) & (df['prev_event'] == "SHOT"), 1, 0)

    # Rebound - less than 2
    df['is_rebound'] = np.where((df['prev_event'] == "SHOT") & (df['Seconds_Elapsed'] - df['prev_seconds'] <= 2.0)
                                & (df['Ev_Team'] == df['prev_ev_team']), 1, 0)

    # Rush shot defined like how Manny does
    df['is_rush'] = np.where(
        ((df['Seconds_Elapsed'] - df['prev_seconds'] <= 4.0) & ((df['Home_Zone'] != df['prev_home_zone']) & (df['prev_home_zone'] != "Neu")))
        |
        ((df['Seconds_Elapsed'] - df['prev_seconds'] <= 4.0) & (df['prev_event'].isin(["TAKE", "GIVE"]))),
        1, 0
    )

    # Non_Shot_rebound - miss or block and less or equal to 2
    df['non_sog_rebound'] = np.where((df['prev_event'].isin(["MISS", "BLOCK"])) & (df['Seconds_Elapsed'] - df['prev_seconds'] <= 2.0)
                                     & (df['Ev_Team'] == df['prev_ev_team']), 1, 0)

    return df


def clean_pbp(pbp):
    """
    Clean the pbp:
    1. Add new columns
    2. Get rid of unnecessary columns
    
    :param pbp: DataFrame for pbp
    
    :return: cleaned up pbp
    """
    print("Clean pbp")

    pbp = pbp.sort_values(['Date', 'Game_Id', 'Period', 'Seconds_Elapsed'], ascending=True)
    pbp = pbp[~pbp.Event.isin(["STOP", "PENL", "PEND"])]

    # Fix Scores!!
    pbp['Home_Score'], pbp["Away_Score"] = zip(*pbp.apply(lambda row: fix_score_cat(row), axis=1))

    # Get rid of shootouts
    pbp.Game_Id = pbp.Game_Id.astype(int)
    pbp.drop(pbp[(pbp['Period'] == 5) & (pbp['Game_Id'] < 30000)].index, inplace=True)

    # Get rid of plays without coordinates
    pbp = pbp[pbp["xC"].notnull()]
    pbp = pbp[pbp["yC"].notnull()]
    pbp = pbp[pbp["xC_adj"].notnull()]
    pbp = pbp[pbp["yC_adj"].notnull()]

    # Get previous event info
    pbp = get_previous_event_info(pbp)

    # "Legal" Strengths
    strengths = ['5x5', '6x5', '5x6', '5x4', '4x5', '5x3', '3x5', '4x3', '4x4', '3x4', '3x3', '6x4', '4x6', '6x3', '3x6']
    pbp = pbp[pbp.Strength.isin(strengths)]

    pbp = pbp[pbp["prev_xC_adj"].notnull()]
    pbp = pbp[pbp["prev_yC_adj"].notnull()]

    # Now just need these event
    pbp = pbp[pbp.Event.isin(["SHOT", "GOAL", "MISS"])]

    # Make score category for home team
    pbp['score_cat'] = np.where(pbp['Home_Score'] - pbp['Away_Score'] >= 3, 3,
                                np.where(pbp['Home_Score'] - pbp['Away_Score'] <= -3, -3, pbp['Home_Score'] - pbp['Away_Score']))

    # Fill nan's with NA
    pbp['Type'].fillna("NA", inplace=True)

    # Misclassify some this way but probably better
    pbp['Distance'] = pbp.apply(lambda row: sqrt(((89.45 - abs(row['xC_adj']))**2 + (row['yC_adj'] ** 2))), axis=1)
    pbp['xC_adj'] = np.where(pbp['xC_adj'] == 0, 1, pbp['xC_adj'])
    pbp['Angle'] = pbp.apply(lambda row: 90 if row['yC_adj'] == 0 else np.arctan((89.45-abs(row['xC_adj']))/row['yC_adj'])
                                                                        * (180/np.pi), axis=1)

    pbp['shooter_hand'], pbp["shooter_pos"] = shared.get_shooter_info(pbp)
    pbp['off_wing'] = pbp.apply(lambda row: if_off_wing(row), axis=1)

    # Adjust if away team who shot it
    pbp['score_cat'] = np.where(pbp['Ev_Team'] == pbp['Home_Team'], pbp['score_cat'], -pbp['score_cat'])
    pbp['Strength'] = pbp.apply(lambda row: fix_strength(row), axis=1)
    pbp['if_home'] = np.where(pbp['Ev_Team'] == pbp['Home_Team'], 1, 0)

    # If Empty net
    pbp['Home_Goalie'].fillna("Empty", inplace=True)
    pbp['Away_Goalie'].fillna("Empty", inplace=True)
    pbp['empty_net'] = pbp.apply(lambda row: if_empty_net(row), axis=1)

    # Fix previous row
    pbp = pbp.apply(lambda row: fix_prev_event(row), axis=1)

    pbp['prev_angle'] = pbp.groupby(['Game_Id', 'Period'])['Angle'].shift(1)
    pbp['angle_change'] = pbp.apply(lambda row: get_angle_change(row), axis=1)
    pbp['Angle'] = abs(pbp['Angle'])
    pbp.drop(['prev_angle'], axis=1, inplace=True)

    # Distance from last event
    pbp['distance_change'] = pbp.apply(lambda x: get_distance(x), axis=1)

    # Get rid of goalies who took shots
    pbp = pbp[pbp.shooter_pos != "G"]

    # Change RW, LW, C --> F
    pbp['if_forward'] = np.where(pbp['shooter_pos'] == "F", 1, 0)

    # Label outcomes
    pbp['Outcome'] = np.where(pbp['Event'] == "GOAL", 2, np.where(pbp['Event'] == "SHOT", 1, np.where(pbp['Event'] == "MISS", 0, 3)))
    pbp = pbp[pbp['Outcome'] != 3]

    # Get rid of duplicates
    pbp.drop_duplicates(['Date', 'Game_Id', 'Period', 'Event', 'Description', 'Seconds_Elapsed'], inplace=True)

    return pbp


def convert_data(pbp, if_shooter):
        """      
        Convert data to suitable format to get probs.
        """
        all_vars = ['off_wing',
                     'Distance', 'Angle',
                     'empty_net',
                     'angle_change',
                     'distance_change', 'time_elapsed',
                     'Type_BACKHAND', 'Type_DEFLECTED', 'Type_SLAP SHOT', 'Type_SNAP SHOT', 'Type_TIP-IN',
                     'Type_WRAP-AROUND', 'Type_WRIST SHOT',
                     'Strength_3x3', 'Strength_3x4', 'Strength_3x5', 'Strength_3x6', 'Strength_4x3', 'Strength_4x4',
                     'Strength_4x5', 'Strength_4x6', 'Strength_5x3', 'Strength_5x4', 'Strength_5x5', 'Strength_5x6',
                     'Strength_6x3', 'Strength_6x4', 'Strength_6x5',
                     'score_cat_-3', 'score_cat_-2', 'score_cat_-1', 'score_cat_0', 'score_cat_1', 'score_cat_2', 'score_cat_3',
                     'if_forward',
                     'if_home',
                     'prev_evTeam_Fac', 'prev_evTeam_NonSog', 'prev_evTeam_NonShot', 'prev_evTeam_Sog',
                     'prev_non_evTeam_Fac', 'prev_non_evTeam_NonSog', 'prev_non_evTeam_NonShot', 'prev_non_evTeam_Sog',
                    ]

        categorical_vars = ['Type', 'score_cat', 'Strength']
        labels = ['Outcome']

        # Add if shooter
        all_vars = all_vars + ["reg_xg"] if if_shooter else all_vars

        df_dummies = pd.get_dummies(pbp, columns=categorical_vars)
        df_dummies = check_columns(df_dummies)
        model_df = df_dummies[all_vars + ["Outcome"]]
        model_df.dropna(inplace=True)

        ############
        foo = df_dummies[["season", "Game_Id", "Date", "Period", "Event", "Description", "Seconds_Elapsed"] + all_vars + labels]
        foo.dropna(inplace=True)
        foo = foo.reset_index(drop=True)
        ############

        model_features = model_df[all_vars].values.tolist()
        model_labels = model_df[labels].values.tolist()

        return model_features, model_labels, foo


def check_columns(data):
    """
    Checks columns for final DataFrame.. If column isn't there it adds it in.
    Ex: adds '6x4' dummy variable if in sample that strength wasn't played
    
    Note: Doesn't apply to building the model but when using it on subset of data
    
    :param data: Data used in model
    
    :return: fixed up df
    """
    cols = ['Type_BACKHAND', 'Type_DEFLECTED', 'Type_SLAP SHOT', 'Type_SNAP SHOT',  'Type_TIP-IN',  'Type_WRAP-AROUND',
            'Type_WRIST SHOT', 'Strength_3x3', 'Strength_3x4', 'Strength_3x5',  'Strength_3x6', 'Strength_4x3',
            'Strength_4x4', 'Strength_4x5', 'Strength_4x6', 'Strength_5x3', 'Strength_5x4', 'Strength_5x5', 'Strength_5x6',
            'Strength_6x3', 'Strength_6x4', 'Strength_6x5', 'score_cat_-3', 'score_cat_-2', 'score_cat_-1', 'score_cat_0',
            'score_cat_1', 'score_cat_2', 'score_cat_3',
            ]

    for col in cols:
        if col not in data.columns:
            # If not there just fill it with zeros
            data[col] = 0

    return data

