"""
Apply xG to a given pbp dataframe
"""
import os
import numpy as np
import pandas as pd
from sklearn.externals import joblib
from xg_probs import regress_players as rp
from xg_probs import clean_data_xg

pd.options.mode.chained_assignment = None  # default='warn'


# TODO: Find out why this is a problem
def fix_reg_xg(row):
    """
    Sometimes it's a tuple -> (0,0)...if it's that for a SHOT/MISS/GOAL change to 1
    
    :param row: Df row
    
    :return: Fixed Col
    """
    if type(row['reg_xg']) is tuple and row['Event'] in ['SHOT', 'MISS', 'GOAL']:
        return 1
    else:
        return row['reg_xg']


def get_xg(pbp):

    # Move to directory of this file to get pkl files
    os.chdir(os.path.dirname(os.path.realpath(__file__)))

    # Clean....
    clean_df = clean_data_xg.clean_pbp(pbp)

    # xG not 3 outcomes...
    clean_df['Outcome'] = np.where(clean_df['Outcome'] == 0, 0, np.where(clean_df['Outcome'] == 1, 0, np.where(clean_df['Outcome'] == 2, 1, 3)))
    clean_df = clean_df[clean_df['Outcome'] != 3]

    # Get Regular xG
    clf = joblib.load("gbm_xg.pkl")
    pbp = get_probs(clf, pbp, clean_df, False)

    ######################################################
    ######################################################
    #   Don't do computations for shooter_xg on server   #
    ######################################################
    ######################################################

    pbp['reg_xg'] = 0
    pbp['shooter_xg'] = 0
    """
    # Get Shooter xG
    pbp = rp.get_player_regress(pbp)

    # Merge reg_xg into cleaned pbp
    pbp['Game_Id'] = pbp['Game_Id'].astype(int)
    merge_df = pbp[["Game_Id", "Date", "Period", "Event", "Seconds_Elapsed", "Description", "reg_xg"]]
    clean_df = pd.merge(clean_df, merge_df, on=["Game_Id", "Date", "Period", "Event", "Description", "Seconds_Elapsed"], how="left")
    clean_df = clean_df.drop_duplicates(subset=["Game_Id", "Date", "Period", "Event", "Description", "Seconds_Elapsed"])

    # Fix reg_xg col
    clean_df['reg_xg'] = clean_df.apply(lambda row: fix_reg_xg(row), axis=1)

    clf = joblib.load("gbm_xg_shooter.pkl")
    pbp = get_probs(clf, pbp, clean_df, True)
    """
    ######################################################
    ######################################################
    ######################################################

    # Move back to old directory
    os.chdir('..')

    return pbp


def get_probs(clf, pbp, cleaned_pbp, if_shooter):
    # Get column to assign probs. to
    col = "xg" if not if_shooter else "shooter_xg"

    # Convert to lists
    features, labels, model_df = clean_data_xg.convert_data(cleaned_pbp, if_shooter)

    preds = clf.predict_proba(features)
    goal_preds = [pred[1] for pred in preds]

    # Add back into cleaned
    model_df[col] = pd.Series(goal_preds)

    # Merge clean into pbp
    pbp = pbp.reset_index(drop=True)
    pbp['Game_Id'] = pbp['Game_Id'].astype(int)
    model_df = model_df[["season", "Game_Id", "Date", "Period", "Event", "Description", "Seconds_Elapsed", col]]
    df = pd.merge(pbp, model_df, on=["season", "Game_Id", "Date", "Period", "Event", "Description", "Seconds_Elapsed"], how="left")
    df.drop_duplicates(subset=["season", "Game_Id", "Date", "Period", "Event", "Description", "Seconds_Elapsed"])

    # Fix when an unblocked shot has a Goal Probability of Nan (just put 5.6%)
    # Add when -> xC is Nan, an unblocked shot, not a shootout, and a valid strength (so no penalty shots)
    strengths = ['5x5', '6x5', '5x6', '5x4', '4x5', '5x3', '3x5', '4x3', '4x4', '3x4', '3x3', '6x4', '4x6', '6x3', '3x6']
    df[col] = np.where((df['xC'].isnull()) & (df['Event'].isin(["SHOT", "MISS", "GOAL"])) & (df['Period'] != 5)
                       & (df['Strength'].isin(strengths)), .056, df[col])

    # Give a xg% of 0 when nan
    df[col] = np.where(np.isnan(df[col]), 0, df[col])

    return df






