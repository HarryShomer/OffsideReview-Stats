"""
This has been fazed out -> Check the Elo Ratings update for more details
"""


import pandas as pd
import numpy as np
import re
from bs4 import BeautifulSoup, SoupStrainer
import hockey_scraper as hs
import helpers

import sys
sys.path.append("..")
from machine_info import *


def get_soup(game_html):
    """
    Uses Beautiful soup to parses the html document.
    Some parsers work for some pages but don't work for others....I'm not sure why so I just try them all here in order

    :param game_html: html doc

    :return: "soupified" html 
    """
    for parser in ["lxml", "html.parser", "html5lib"]:
        strainer = SoupStrainer('table', attrs={'id': ['Visitor', 'Home']})
        soup = BeautifulSoup(game_html, parser, parse_only=strainer)

        if soup:
            break

    return soup


def get_game_info(soup):
    """
    Using the "souped-up" file, get the final scores for both teams and the team names
    """
    teams_info = {}

    # Scores
    score_soup = soup.find_all('td', {'align': 'center', 'style': "font-size: 40px;font-weight:bold"})
    teams_info['Visitor_Score'] = int(score_soup[0].get_text())
    teams_info['Home_Score'] = int(score_soup[1].get_text())

    # Team Name
    team_soup = soup.find_all('td', {'align': 'center', 'style': "font-size: 10px;font-weight:bold"})
    regex = re.compile(r'>(.*)<br/?>')
    teams_info['Visitor_Team'] = helpers.TEAMS[regex.findall(str(team_soup[0]))[0]]
    teams_info['Home_Team'] = helpers.TEAMS[regex.findall(str(team_soup[1]))[0]]

    return teams_info


def get_outcomes(from_date, to_date):
    """
    Get outcome for a date range
    
    :param from_date: Date we are getting outcomes from
    :param to_date: Date we are getting to
    
    :return: DataFrame of outcomes
    """
    hs.shared.docs_dir = scraper_data_dir
    schedule = hs.json_schedule.scrape_schedule(from_date, to_date)

    games_list = []
    for game in schedule:
        file = hs.html_pbp.get_pbp(game[0])
        soup = get_soup(file)

        game_info = get_game_info(soup)
        game_info['Game_Id'] = game[0]
        games_list.append(game_info)

    # Convert games just processed into outcomes
    outcomes_df = pd.DataFrame(games_list)
    outcomes_df['Winner'] = np.where(outcomes_df['Home_Score'] > outcomes_df['Visitor_Score'], outcomes_df['Home_Team'],
                                     outcomes_df['Visitor_Team'])

    return outcomes_df


def merge_outcomes(stats_df, outcomes_df):
    """
    Merge the outcomes from each game into the main DataFrame
    """
    outcomes_df["if_home_win"] = outcomes_df.apply(lambda x: 1 if x['Winner'] == x['Home_Team'] else 0, axis=1)

    outcomes_df = outcomes_df.rename(index=str, columns={"Game_Id": "game_id"})
    outcomes_df = outcomes_df[["game_id", "if_home_win"]]

    return pd.merge(stats_df, outcomes_df, how="left", on=['game_id'])

