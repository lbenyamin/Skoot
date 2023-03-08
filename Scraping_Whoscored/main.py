from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from tqdm.auto import tqdm
from random import uniform

import datetime
import pandas as pd
import numpy as np
import time

import joblib
import re

def convert_incident_df(incidents_list) -> pd.DataFrame:
    df = pd.DataFrame(incidents_list, columns=[
        'player_name', 'is_home', 'incident_type', 'incident_minute', 
        'incident_second', 'player_id', 'team_id', 'season', 'description', 'url'
    ])
    df['match_id'] = df['url'].str.split('/', expand=True)[4]
    return df

def convert_schedule_df(schedule_list) -> pd.DataFrame:
    col = ['season', 'date', 'kick_off', 'status', 'home_team', 
           'home_team_id', 'final_score', 'away_team', 'away_team_id', 'match_url']
    df = pd.DataFrame(schedule_list, columns=col)
    
    df['date'] = df['date'].astype(str)
    df['home_team_id'] = df['home_team_id'].astype(int)
    df['away_team_id'] = df['away_team_id'].astype(int)
    df['match_id'] = df['match_url'].str.split('/', expand=True)[4].astype(int)
    df.sort_values(['season', 'date', 'kick_off'], ascending=False, inplace=True)
    
    return df

def convert_match_stats_df(match_stats_list) -> pd.DataFrame:
    match_sum_col = ['match_id', 'season', 'date', 'kick_off', 'home_team', 'home_team_id', 'away_team', 'away_team_id', 'final_score', 
                     'half_time_score', 'full_time_score', 'home_formation', 'away_formation', 'is_home_player']
    sum_col = ['player_name', 'player_id', 'age', 'position', 'DuelAerialWon', 'Touches', 'rating']
    off_col = ['ShotsTotal', 'ShotOnTarget', 'DribbleWon', 'FoulGiven', 
               'OffsideGiven', 'Dispossessed', 'Turnover']
    def_col = ['TackleWonTotal', 'InterceptionAll', 'ClearanceTotal', 'ShotBlocked', 'FoulCommitted']
    pass_col = ['KeyPassTotal', 'TotalPasses', 'PassSuccessInMatch', 'PassCrossTotal', 'PassCrossAccurate', 
                'PassLongBallTotal', 'PassLongBallAccurate', 'PassThroughBallTotal', 'PassThroughBallAccurate']
    all_col = match_sum_col + sum_col + off_col + def_col + pass_col + ['url']
    df = pd.DataFrame(match_stats_list, columns=all_col)
    
    return df

def process_stats_df(df):
    '''Replace "-" with 0s and change data type to int or float accordingly  '''
    
    mapping = {'-': 0} 
    
    replace_dict = {}
    
    num_cols = [
                'match_id', 'home_team_id', 'away_team_id', 'player_id', 'age', 'DuelAerialWon',
                'Touches', 'rating', 'ShotsTotal', 'ShotOnTarget', 'DribbleWon', 'FoulGiven', 
                'OffsideGiven', 'Dispossessed', 'Turnover', 'TackleWonTotal', 'InterceptionAll', 
                'ClearanceTotal', 'ShotBlocked', 'FoulCommitted', 'KeyPassTotal', 'TotalPasses', 
                'PassSuccessInMatch', 'PassCrossTotal', 'PassCrossAccurate', 'PassLongBallTotal',
                'PassLongBallAccurate', 'PassThroughBallTotal', 'PassThroughBallAccurate'
    ]
    
    for col in df.columns:
        replace_dict[col] = mapping
        if col in num_cols:
            try:
                df[col] = df[col].astype(int)
            except:
                try:
                    df[col] = df[col].astype(float)
                except:
                    continue
                
    df['date'] = pd.to_datetime(df['date']).astype(str)
    df = df.sort_values(['date', 'kick_off', 'match_id'])
    df.reset_index(drop=True, inplace=True)
            
    return df.replace(replace_dict)

def scrape_team_url(url, driver=None, options=None, url_loaded=False) -> list:
    '''Accept league url and return team url'''
    if options is None:
        options = webdriver.ChromeOptions()
        options.add_argument('log-level=3')
        # options.add_argument("--headless")
    if driver is None:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    if not url_loaded:
        driver.get(url)
    else:
        url = driver.current_url
        
    team_stats_button = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#sub-navigation > ul > li:nth-child(3) > a')))
    team_stats_button.click()

    team_table = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#top-team-stats-summary-content')))

    team_elem_list = team_table.find_elements(By.TAG_NAME, 'a')

    return [a.get_attribute('href') for a in team_elem_list]

def scrape_league_url(driver=None, options=None):
    if options is None:
        options = webdriver.ChromeOptions()
        options.add_argument('log-level=3')
        # options.add_argument("--headless")
    if driver is None:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get('https://whoscored.com/')

    all_league_button = driver.find_elements(By.CSS_SELECTOR, \
                                             '#tournament-groups > li.col12-lg-3.col12-m-4.col12-s-4.col12-xs-2 > a')[0]
    all_league_button.click()
    letters_bar = driver.find_elements(By.CSS_SELECTOR, '#domestic-index')[0]

    # initialize dict to store all region tournament urls
    regions_dict = {}
    # loop through each letter, skip first == international
    for letter in letters_bar.find_elements(By.TAG_NAME, 'a')[1:]:
        letter.click()
        # get all domestic regions
        domestic_regions_elem = driver.find_elements(By.CSS_SELECTOR, '#domestic-regions')[0]
        # get all sub regions
        sub_regions_elem_list = domestic_regions.find_elements(By.CLASS_NAME, 'regions')
        # loop through each sub regions
        for sub_regions_elem in sub_regions_elem_list:
            if sub_regions_elem.text == '':
                continue
            sub_regions_list = re.split(r'\n| and ', sub_regions_elem.text)
            tournament_url_list = sub_regions_elem.find_elements(By.CLASS_NAME, 't')
            region_url_list = []
            count = 0
            prev_id = tournament_url_list[0].get_attribute('href').split('Regions/')[1].split('/')[0]
            for elem in tournament_url_list:
                url = elem.get_attribute('href')
                region_id = url.split('Regions/')[1].split('/')[0]
                if region_id == prev_id:
                    region_url_list.append(url)
                else:
                    regions_dict[sub_regions_list[count]] = region_url_list
                    count+=1
                    region_url_list = []
                    region_url_list.append(url)
                prev_id = region_id
                regions_dict[sub_regions_list[count]] = region_url_list
    return regions_dict

def scrape_schedule_single_season(season=None, url=None, driver=None, options=None, url_loaded=False) -> pd.DataFrame:
    '''Scrape schedules of a given league, return DataFrame'''
    if options is None:
        options = webdriver.ChromeOptions()
        options.add_argument('log-level=3')
        # options.add_argument("--headless")
    if driver is None:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    if not url_loaded:
        driver.get(url)
    else:
        url = driver.current_url

    season_match_sche_list = []

    for season_elem in driver.find_element(By.CSS_SELECTOR, '#seasons').find_elements(By.TAG_NAME, 'option'):
        if season_elem.text == season:
            season_elem.click()
            break
        else:
            continue
    date_controller = WebDriverWait(driver,15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '#date-controller')))[0]
    prev_week_button = date_controller.find_elements(By.TAG_NAME, 'a')[0]
    next_week_button = date_controller.find_elements(By.TAG_NAME, 'a')[2]
    no_data_list = [
        'No data for previous month',
        'No data for previous week',
        'No data for next month',
        'No data for next week'
    ]
    click_prev = True
    while click_prev:
        prev_week_button.click()
        if prev_week_button.get_attribute('title') in no_data_list:
            click_prev == False
            break

    keep_clicking = True
    while keep_clicking:
        try:
            match_table = WebDriverWait(driver,15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '#tournament-fixture > div')))[0]
            rows = WebDriverWait(match_table,15).until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'divtable-row')))
            week_list = []
            for row in rows:
                try:
                    date = pd.to_datetime(row.text).date()
                except:
                    kick_off = WebDriverWait(row,5).until(EC.presence_of_element_located((By.CLASS_NAME, 'time'))).text

                    status = WebDriverWait(row,5).until(EC.presence_of_element_located((By.CLASS_NAME, 'status'))).text
                    if status == ' ':
                        status = 'upcoming'

                    home_team_elem = WebDriverWait(row,5).until(EC.presence_of_element_located((By.CLASS_NAME, 'home'))).find_element(By.TAG_NAME, 'a')
                    home_team = home_team_elem.text
                    home_team_id = home_team_elem.get_attribute('href').split('/')[4]

                    away_team_elem = WebDriverWait(row,5).until(EC.presence_of_element_located((By.CLASS_NAME, 'away'))).find_element(By.TAG_NAME, 'a')
                    away_team = away_team_elem.text
                    away_team_id = away_team_elem.get_attribute('href').split('/')[4]

                    result_elem = WebDriverWait(row,5).until(EC.presence_of_element_located((By.CLASS_NAME, 'result')))
                    result = result_elem.text
                    match_url = WebDriverWait(result_elem,5).until(EC.presence_of_element_located((By.TAG_NAME, 'a'))).get_attribute('href')
                    week_list+=[[season, date, kick_off, status, home_team, home_team_id, result, away_team, away_team_id, match_url]]
        except:
            time.sleep(uniform(2, 5))
            match_table = WebDriverWait(driver,15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '#tournament-fixture > div')))[0]
            rows = WebDriverWait(match_table,15).until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'divtable-row')))
            week_list = []
            for row in rows:
                try:
                    date = pd.to_datetime(row.text).date()
                except:
                    kick_off = WebDriverWait(row,5).until(EC.presence_of_element_located((By.CLASS_NAME, 'time'))).text

                    status = WebDriverWait(row,5).until(EC.presence_of_element_located((By.CLASS_NAME, 'status'))).text
                    if status == ' ':
                        status = 'upcoming'

                    home_team_elem = WebDriverWait(row,5).until(EC.presence_of_element_located((By.CLASS_NAME, 'home'))).find_element(By.TAG_NAME, 'a')
                    home_team = home_team_elem.text
                    home_team_id = home_team_elem.get_attribute('href').split('/')[4]

                    away_team_elem = WebDriverWait(row,5).until(EC.presence_of_element_located((By.CLASS_NAME, 'away'))).find_element(By.TAG_NAME, 'a')
                    away_team = away_team_elem.text
                    away_team_id = away_team_elem.get_attribute('href').split('/')[4]

                    result_elem = WebDriverWait(row,5).until(EC.presence_of_element_located((By.CLASS_NAME, 'result')))
                    result = result_elem.text
                    match_url = WebDriverWait(result_elem,5).until(EC.presence_of_element_located((By.TAG_NAME, 'a'))).get_attribute('href')
                    week_list+=[[season, date, kick_off, status, home_team, home_team_id, result, away_team, away_team_id, match_url]]
        season_match_sche_list+=week_list
        if next_week_button.get_attribute('title') in no_data_list:
            keep_clicking = False
            break
        next_week_button.click()
    
    return season_match_sche_list

def scrape_schedule_multi_season(seasons_list=None, url=None, driver=None, options=None, supervised=True) -> list:
    if options is None:
        options = webdriver.ChromeOptions()
        options.add_argument('log-level=3')
        # options.add_argument("--headless")

    all_schedule_list = []

    seasons_to_scrape = seasons_list
    if driver is None:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    keep_scraping = True
    total_iter = 0
    season_success = []
    while keep_scraping:
        total_iter+=1
        season_fail = []
        prev_season_successful = True
        consecutive_fail_num = 0
        for season in tqdm(seasons_to_scrape):
            single_season_schedule_list = []
            try:
                try:
                    driver.current_url
                except:
                    print("Driver closed. Scraping stopped.")
                    keep_scraping = False
                    break
                    
                single_season_schedule_list+=scrape_schedule_single_season(season=season, url=url, driver=driver, options=options, url_loaded=True)
                prev_url_successful = True
                consecutive_fail_num = 0
                season_success.append(season)
            except:
                try:
                    time.sleep(uniform(3, 6))
                    single_season_schedule_list+=scrape_schedule_single_season(season=season, url=url, driver=driver, options=options)
                    prev_season_successful = True
                    consecutive_fail_num = 0
                    season_success.append(season)
                except:
                    season_fail.append(season)
                    print(f"total seasons missed {len(season_fail)}")
                    
                    if prev_season_successful is False:
                        consecutive_fail_num+=1
                        print(f"consecutive seasons missed {consecutive_fail_num}")
                        if consecutive_fail_num >= 2 and supervised is False:
                            keep_scraping = False
                            break
                        elif consecutive_fail_num >= 3 and supervised is True:
                            keep_scraping = False
                            break
                    prev_season_successful = False
                    continue
            all_schedule_list+=single_season_schedule_list
        if total_iter >= 2:
            keep_scraping = False
        if season_fail != []:
            seasons_to_scrape = season_fail
        else:
            keep_scraping = False
        print(f"Total Iter: {total_iter}")
    season_missed = [a for a in seasons_list if a not in season_success]
    print(f"Scrape Complete. Total iter: {total_iter}. Season failed: {len(season_fail)}. Season succeeded: {len(season_success)}. Total missed: {len(season_missed)}")
    return all_schedule_list, season_missed

def scrape_incidents_single_match(url=None, driver=None, options=None, url_loaded=False) -> list:
    if options is None:
        options = webdriver.ChromeOptions()
        options.add_argument('log-level=3')
        # options.add_argument("--headless")
    if driver is None:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    if url_loaded:
        url = driver.current_url
    else:
        driver.get(url)
        
    match_id = int(url.split('/')[4])
    season = WebDriverWait(driver,5).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#breadcrumb-nav > a'))).text.split(' ')[2]
    player_sum_page = WebDriverWait(driver,5).until(EC.presence_of_element_located((By.CSS_SELECTOR, \
                                                '#sub-sub-navigation > ul > li:nth-child(1) > a')))
    incidents_elem_table = WebDriverWait(driver,5).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#live-incidents')))

    incidents_list = []

    incidents_elem_row_list = WebDriverWait(incidents_elem_table,5).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'tr')))
    for row_elem in incidents_elem_row_list:
        row_sub_list = WebDriverWait(row_elem, 5).until(EC.presence_of_all_elements_located((By.TAG_NAME, 'td')))
        for i in [0, 2]:
            if row_sub_list[i].text == '':
                continue
            if i == 0:
                is_home_incident = True
            else:
                is_home_incident = False

            cell_list = WebDriverWait(row_sub_list[i], 5).until(EC.presence_of_all_elements_located((By.CLASS_NAME, \
                                                                    'match-centre-header-team-key-incident')))
            for incident_elem in cell_list:

                player_name = WebDriverWait(incident_elem, 5).until(EC.presence_of_element_located((By.TAG_NAME, 'a'))).text

                if player_name == '':
                    continue

                incident_data = incident_elem.find_element(By.TAG_NAME, 'div')
                incident_minute = incident_data.get_attribute('data-minute')
                incident_second = incident_data.get_attribute('data-second')
                incident_player_id = incident_data.get_attribute('data-player-id')
                incident_team_id = incident_data.get_attribute('data-team-id')
                incident_type = incident_data.get_attribute('data-type')
                incident_des = incident_elem.get_attribute('title')

                player_incident_list = [
                    player_name, is_home_incident, int(incident_type), int(incident_minute), int(incident_second),
                    int(incident_player_id), int(incident_team_id), season, incident_des, url
                ]

                incidents_list.append(player_incident_list)
    return incidents_list

def scrape_incidents_full_season(league=None, season=None, driver=None, options=None, supervised=True, url_list=None):
    if options is None:
        options = webdriver.ChromeOptions()
        options.add_argument('log-level=3')
        # options.add_argument("--headless")

    full_season_incidents_list = []
    if url_list is None:
        url_to_scrape = league[season]
    else:
        url_to_scrape = url_list
    if driver is None:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    keep_scraping = True
    total_iter = 0
    url_success = []
    while keep_scraping:
        total_iter+=1
        url_fail = []
        prev_url_successful = True
        consecutive_fail_num = 0
        for url in tqdm(url_to_scrape):
            try:
                driver.current_url
            except:
                print("Driver closed. Scraping stopped.")
                keep_scraping = False
                break
            try:
                full_season_incidents_list+=scrape_incidents_single_match(url, driver=driver, options=options)
                prev_url_successful = True
                consecutive_fail_num = 0
                url_success.append(url)
            except:
                try:
                    time.sleep(uniform(3, 6))
                    full_season_incidents_list+=scrape_incidents_single_match(url, driver=driver, options=options)
                    prev_url_successful = True
                    consecutive_fail_num = 0
                    url_success.append(url)
                except:
                    url_fail.append(url)
                    print(f"total url missed {len(url_fail)}")
                    
                    if prev_url_successful is False:
                        consecutive_fail_num+=1
                        print(f"consecutive url missed {consecutive_fail_num}")
                        if consecutive_fail_num >= 10 and supervised is False:
                            keep_scraping = False
                            break
                        elif consecutive_fail_num >= 30 and supervised is True:
                            keep_scraping = False
                            break
                    prev_url_successful = False
                    continue
        if supervised is False and total_iter >= 2:
            keep_scraping = False
        elif supervised is True and total_iter >= 3:
            keep_scraping = False
        if url_fail != []:
            url_to_scrape = url_fail
        else:
            keep_scraping = False
        print(f"Total Iter: {total_iter}")
    url_missed = [a for a in url_to_scrape if a not in url_success]
    print(f"Scrape Complete. Total iter: {total_iter}. Total url fail: {len(url_fail)}. Total url success: {len(url_success)}")
    return full_season_incidents_list, url_missed

def incidents_super_scrape(driver=None, options=None, supervised=True, url_list=None, chunk_size=400) -> list:
    if options is None:
        options = webdriver.ChromeOptions()
        options.add_argument('log-level=3')
        # options.add_argument("--headless")
    if driver is None:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    all_scraped_list = []
    all_missed_list = []
    chunked_list = [url_list[i:i+chunk_size] for i in range(0, len(url_list), chunk_size)]
    for i, chunk in enumerate(tqdm(chunked_list)):
        try:
            driver.current_url
        except:
            print("Driver closed. Scraping stopped.")
            break
        scraped_list, missed_list = scrape_incidents_full_season(driver=driver, options=options, supervised=supervised, url_list=chunk)
        all_scraped_list+=scraped_list
        all_missed_list+=missed_list
        
    if i != len(chunked_list)-1:
        for ii in range(len(chunked_list) -1 - i):
            all_missed_list+=chunked_list[i+ii+1]
    print(f"Super scrape complete. Missed: {len(all_missed_list)}")
    return all_scraped_list, all_missed_list

def scrape_player_stats_single_match(url=None, season=None, driver=None, options=None, cur_date=None, url_loaded=False) -> list:
    if cur_date is None:
        cur_date = datetime.date.today()
    if options is None:
        options = webdriver.ChromeOptions()
        options.add_argument('log-level=3')
        # options.add_argument("--headless")
    if driver is None:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    if not url_loaded:
        driver.get(url)
    else:
        url = driver.current_url
    if season is None:
        season = WebDriverWait(driver,5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '#breadcrumb-nav > a'))
        ).text.split(' ')[2]
    
    match_id = int(url.split('/')[4])

    ## Match summary
    home_team_selector = '#match-header > table > tbody > tr:nth-child(1) > td:nth-child(1) > a'
    away_team_selector = '#match-header > table > tbody > tr:nth-child(1) > td:nth-child(3) > a'
    final_score_selector = '#match-header > table > tbody > tr:nth-child(1) > td.result'
    time_elapsed_selector = '#match-header > table > tbody > tr:nth-child(2) > td:nth-child(2) > div:nth-child(1) > dl > dd > span'
    half_time_score_selector = '#match-header > table > tbody > tr:nth-child(2) > td:nth-child(2) > div:nth-child(2) > dl > dd:nth-child(2)'
    full_time_score_selector = '#match-header > table > tbody > tr:nth-child(2) > td:nth-child(2) > div:nth-child(2) > dl > dd:nth-child(4)'
    kick_off_selector = '#match-header > table > tbody > tr:nth-child(2) > td:nth-child(2) > div:nth-child(3) > dl > dd:nth-child(2)'
    date_selector = '#match-header > table > tbody > tr:nth-child(2) > td:nth-child(2) > div:nth-child(3) > dl > dd:nth-child(4)'
    
    try:
        date = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR, date_selector))).text
    except:
        date = np.nan
    try:
        home_team = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR, home_team_selector))).text
    except:
        home_team = np.nan
    try:
        home_team_id = WebDriverWait(driver,7).until(EC.presence_of_element_located((By.CSS_SELECTOR, home_team_selector))).get_attribute('href').split('/')[4]
    except:
        home_team_id = np.nan
    try:
        away_team = WebDriverWait(driver,7).until(EC.presence_of_element_located((By.CSS_SELECTOR, away_team_selector))).text
    except:
        away_team = np.nan
    try:
        away_team_id = WebDriverWait(driver,7).until(EC.presence_of_element_located((By.CSS_SELECTOR, away_team_selector))).get_attribute('href').split('/')[4]
    except:
        away_team_id = np.nan
    try:
        final_score = WebDriverWait(driver,5).until(EC.presence_of_element_located((By.CSS_SELECTOR, final_score_selector))).text
    except:
        final_score = np.nan
    try:
        time_elapsed = WebDriverWait(driver,5).until(EC.presence_of_element_located((By.CSS_SELECTOR, time_elapsed_selector))).text
    except:
        time_elapsed = np.nan
    try:
        half_time_score = WebDriverWait(driver,3).until(EC.presence_of_element_located((By.CSS_SELECTOR, half_time_score_selector))).text
    except:
        half_time_score = np.nan
    try:
        full_time_score = WebDriverWait(driver,3).until(EC.presence_of_element_located((By.CSS_SELECTOR, full_time_score_selector))).text
    except:
        full_time_score = np.nan
    try:
        kick_off = WebDriverWait(driver,3).until(EC.presence_of_element_located((By.CSS_SELECTOR, kick_off_selector))).text
    except:
        kick_off = np.nan

    match_sum_col = ['match_id', 'season', 'date', 'kick_off', 'home_team', 'home_team_id', 'away_team', 'away_team_id', 'final_score', 
                     'half_time_score', 'full_time_score', 'home_formation', 'away_formation', 'is_home_player']
    sum_col = ['player_name', 'player_id', 'age', 'position', 'DuelAerialWon', 'Touches', 'rating']
    off_col = ['ShotsTotal', 'ShotOnTarget', 'DribbleWon', 'FoulGiven', 
               'OffsideGiven', 'Dispossessed', 'Turnover']
    def_col = ['TackleWonTotal', 'InterceptionAll', 'ClearanceTotal', 'ShotBlocked', 'FoulCommitted']
    pass_col = ['KeyPassTotal', 'TotalPasses', 'PassSuccessInMatch', 'PassCrossTotal', 'PassCrossAccurate', 
                'PassLongBallTotal', 'PassLongBallAccurate', 'PassThroughBallTotal', 'PassThroughBallAccurate']

    is_home_player = True

    home_formation = WebDriverWait(driver,15).until(EC.presence_of_element_located((By.CSS_SELECTOR, \
                            '#match-centre-header > div:nth-child(1) > div.team-info > div.formation'))).text
    away_formation = WebDriverWait(driver,15).until(EC.presence_of_element_located((By.CSS_SELECTOR, \
                            '#match-centre-header > div:nth-child(3) > div.team-info > div.formation'))).text
    
    
    match_sum_arr = np.array([match_id, season, date, kick_off, home_team, home_team_id, away_team, away_team_id, final_score, 
                              half_time_score, full_time_score, home_formation, away_formation, is_home_player])

    player_stats_page = WebDriverWait(driver,15).until(EC.presence_of_element_located((By.CSS_SELECTOR, \
                                                '#sub-sub-navigation > ul > li:nth-child(2) > a')))
    player_stats_page.click()
    
    ## Player Stats
    # loop through home and away stats
    for side in ['home', 'away']:
        if side == 'home':
            side_xpath_index = 1
        else:
            side_xpath_index = 3

        sum_stats_body_xpath = f'/html/body/div[5]/div[3]/div[3]/div[{side_xpath_index}]/div[2]/div[2]/div/table/tbody'
        off_stats_body_xpath = f'/html/body/div[5]/div[3]/div[3]/div[{side_xpath_index}]/div[4]/div[2]/div/table/tbody'
        def_stats_body_xpath = f'/html/body/div[5]/div[3]/div[3]/div[{side_xpath_index}]/div[3]/div[2]/div/table/tbody'
        pass_stats_body_xpath = f'/html/body/div[5]/div[3]/div[3]/div[{side_xpath_index}]/div[5]/div[2]/div/table/tbody'    
        sum_button_selector = f'#live-player-{side}-options > li:nth-child(1) > a'
        off_button_selector = f'#live-player-{side}-options > li:nth-child(2) > a'
        def_button_selector = f'#live-player-{side}-options > li:nth-child(3) > a'
        pass_button_selector = f'#live-player-{side}-options > li:nth-child(4) > a'
        sum_button_xpath = f'/html/body/div[5]/div[3]/div[3]/div[{side_xpath_index}]/div[1]/ul/li[1]/a'
        off_button_xpath = f'/html/body/div[5]/div[3]/div[3]/div[{side_xpath_index}]/div[1]/ul/li[2]/a'
        def_button_xpath = f'/html/body/div[5]/div[3]/div[3]/div[{side_xpath_index}]/div[1]/ul/li[3]/a'
        pass_button_xpath = f'/html/body/div[5]/div[3]/div[3]/div[{side_xpath_index}]/div[1]/ul/li[4]/a'
        sum_table_selector = f'#statistics-table-{side}-summary'
        off_table_selector = f'#statistics-table-{side}-offensive'
        def_table_selector = f'#statistics-table-{side}-defensive'
        pass_table_selector = f'#statistics-table-{side}-passing'
        stats_body_selector = '#player-table-statistics-body'

        #### Summary
        summary_button = WebDriverWait(driver,10).until(EC.presence_of_element_located((By.CSS_SELECTOR, sum_button_selector)))
        summary_button.click()
        sum_table = WebDriverWait(driver,15).until(EC.presence_of_element_located((By.CSS_SELECTOR, sum_table_selector)))
        sum_stats_body = WebDriverWait(sum_table,5).until(EC.presence_of_element_located((By.CSS_SELECTOR, stats_body_selector)))
        row_elem_list = WebDriverWait(sum_stats_body,15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'tr')))

        sum_arr = []
        for row in row_elem_list:
            player_name =  row.find_elements(By.TAG_NAME, 'span')[0].text
            player_id = row.find_element(By.TAG_NAME, 'a').get_attribute('href').split('/')[4]
            age = row.find_elements(By.TAG_NAME, 'span')[3].text
            position = row.find_elements(By.TAG_NAME, 'span')[4].text.split(' ')[1]
            DuelAerialWon = row.find_element(By.CLASS_NAME, 'DuelAerialWon').text
            Touches = row.find_element(By.CLASS_NAME, 'Touches').text
            rating = row.find_element(By.CLASS_NAME, 'rating').text
            
            sum_arr += [[player_name, player_id, age, position, DuelAerialWon, Touches, rating]]
        sum_arr = np.array(sum_arr)

        #### Offensive
        off_button = WebDriverWait(driver,5).until(EC.presence_of_element_located((By.CSS_SELECTOR, off_button_selector)))
        off_button.click()
        off_table = WebDriverWait(driver,15).until(EC.presence_of_element_located((By.CSS_SELECTOR, off_table_selector)))
        off_stats_body = WebDriverWait(off_table,15).until(EC.presence_of_element_located((By.CSS_SELECTOR, stats_body_selector)))
        row_elem_list = WebDriverWait(off_stats_body,15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'tr')))

        off_arr = []
        for row in row_elem_list:
            ShotsTotal = row.find_element(By.CLASS_NAME, 'ShotsTotal').text
            ShotOnTarget = row.find_element(By.CLASS_NAME, 'ShotOnTarget').text
            DribbleWon = row.find_element(By.CLASS_NAME, 'DribbleWon').text
            FoulGiven = row.find_element(By.CLASS_NAME, 'FoulGiven').text
            OffsideGiven = row.find_element(By.CLASS_NAME, 'OffsideGiven').text
            Dispossessed = row.find_element(By.CLASS_NAME, 'Dispossessed').text
            Turnover = row.find_element(By.CLASS_NAME, 'Turnover').text

            off_arr += [[ShotsTotal, ShotOnTarget, DribbleWon, FoulGiven,
                             OffsideGiven, Dispossessed, Turnover]]

        off_arr = np.array(off_arr)
        #### Defensive

        def_button = WebDriverWait(driver,5).until(EC.presence_of_element_located((By.CSS_SELECTOR, def_button_selector)))
        def_button.click()


        def_table = WebDriverWait(driver,15).until(EC.presence_of_element_located((By.CSS_SELECTOR, def_table_selector)))
        def_stats_body = WebDriverWait(def_table,15).until(EC.presence_of_element_located((By.CSS_SELECTOR, stats_body_selector)))
        row_elem_list = WebDriverWait(def_stats_body,15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'tr')))

        def_arr = []
        for row in row_elem_list:
            TackleWonTotal = row.find_element(By.CLASS_NAME, 'TackleWonTotal').text
            InterceptionAll = row.find_element(By.CLASS_NAME, 'InterceptionAll').text
            ClearanceTotal = row.find_element(By.CLASS_NAME, 'ClearanceTotal').text
            ShotBlocked = row.find_element(By.CLASS_NAME, 'ShotBlocked').text
            FoulCommitted = row.find_element(By.CLASS_NAME, 'FoulCommitted').text

            def_arr += [[TackleWonTotal, InterceptionAll, ClearanceTotal, ShotBlocked, FoulCommitted]]
        def_arr = np.array(def_arr)
        
        #### Passing
        pass_button = WebDriverWait(driver,5).until(EC.presence_of_element_located((By.CSS_SELECTOR, pass_button_selector)))
        pass_button.click()
        pass_table = WebDriverWait(driver,15).until(EC.presence_of_element_located((By.CSS_SELECTOR, pass_table_selector)))
        pass_stats_body = WebDriverWait(pass_table,15).until(EC.presence_of_element_located((By.CSS_SELECTOR, stats_body_selector)))
        row_elem_list = WebDriverWait(pass_stats_body,15).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'tr')))
        pass_arr = []
        for row in row_elem_list:
            KeyPassTotal = row.find_element(By.CLASS_NAME, 'KeyPassTotal').text
            TotalPasses = row.find_element(By.CLASS_NAME, 'TotalPasses').text
            PassSuccessInMatch = row.find_element(By.CLASS_NAME, 'PassSuccessInMatch').text
            PassCrossTotal = row.find_element(By.CLASS_NAME, 'PassCrossTotal').text
            PassCrossAccurate = row.find_element(By.CLASS_NAME, 'PassCrossAccurate').text
            PassLongBallTotal = row.find_element(By.CLASS_NAME, 'PassLongBallTotal').text
            PassLongBallAccurate = row.find_element(By.CLASS_NAME, 'PassLongBallAccurate').text
            PassThroughBallTotal = row.find_element(By.CLASS_NAME, 'PassThroughBallTotal').text
            PassThroughBallAccurate = row.find_element(By.CLASS_NAME, 'PassThroughBallAccurate').text

            pass_arr += [[KeyPassTotal, TotalPasses, PassSuccessInMatch, PassCrossTotal, PassCrossAccurate, 
                        PassLongBallTotal, PassLongBallAccurate, PassThroughBallTotal, PassThroughBallAccurate, url]]

        pass_arr = np.array(pass_arr)

        # combine all stats categories
        all_col = match_sum_col + sum_col + off_col + def_col + pass_col + ['url']

        if side == 'home':
            match_sum_arr_rep = np.repeat([match_sum_arr], len(sum_arr), axis=0)
            home_stats_arr = np.concatenate([match_sum_arr_rep, sum_arr, off_arr, def_arr, pass_arr], axis=1)
        else:
            match_sum_arr[-1] = False
            match_sum_arr_rep = np.repeat([match_sum_arr], len(sum_arr), axis=0)
            away_stats_arr = np.concatenate([match_sum_arr_rep, sum_arr, off_arr, def_arr, pass_arr], axis=1)

    match_stats_arr = np.concatenate([home_stats_arr, away_stats_arr])

    return match_stats_arr.tolist()

def scrape_player_stats_full_season(season=None, driver=None, options=None, supervised=True, url_list=None) -> list:
    if options is None:
        options = webdriver.ChromeOptions()
        options.add_argument('log-level=3')
        # options.add_argument("--headless")

    full_season_match_list = []

    url_to_scrape = url_list
    if driver is None:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    keep_scraping = True
    total_iter = 0
    url_success = []
    while keep_scraping:
        total_iter+=1
        url_fail = []
        prev_url_successful = True
        consecutive_fail_num = 0
        for url in tqdm(url_to_scrape):
            try:
                try:
                    driver.current_url
                except:
                    print("Driver closed. Scraping stopped.")
                    keep_scraping = False
                    break
                full_season_match_list+=scrape_player_stats_single_match(url=url, season=season, driver=driver, options=options)
                prev_url_successful = True
                consecutive_fail_num = 0
                url_success.append(url)
            except:
                try:
                    time.sleep(uniform(3, 6))
                    full_season_match_list+=scrape_player_stats_single_match(url=url, season=season, driver=driver, options=options)
                    prev_url_successful = True
                    consecutive_fail_num = 0
                    url_success.append(url)
                except:
                    url_fail.append(url)
                    print(f"total url missed {len(url_fail)}")
                    
                    if prev_url_successful is False:
                        consecutive_fail_num+=1
                        print(f"consecutive url missed {consecutive_fail_num}")
                        if consecutive_fail_num >= 10 and supervised is False:
                            keep_scraping = False
                            break
                        elif consecutive_fail_num >= 30 and supervised is True:
                            keep_scraping = False
                            break
                    prev_url_successful = False
                    continue
        if total_iter >= 2:
            keep_scraping = False
        if url_fail != []:
            url_to_scrape = url_fail
        else:
            keep_scraping = False
        print(f"Total Iter: {total_iter}")
    url_missed = [a for a in url_list if a not in url_success]
    print(f"Scrape Complete. Total iter: {total_iter}. Total url fail: {len(url_fail)}. Total url success: {len(url_success)}")
    return full_season_match_list, url_missed

def player_stats_super_scrape(driver=None, options=None, supervised=True, url_list=None, chunk_size=400) -> list:
    if options is None:
        options = webdriver.ChromeOptions()
        options.add_argument('log-level=3')
        # options.add_argument("--headless")
    if driver is None:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    all_scraped_list = []
    all_missed_list = []
    chunked_list = [url_list[i:i+chunk_size] for i in range(0, len(url_list), chunk_size)]
    for i, chunk in enumerate(tqdm(chunked_list)):
        try:
            driver.current_url
        except:
            print("Driver closed. Scraping stopped.")
            break
        scraped_list, missed_list = scrape_player_stats_full_season(driver=driver, options=options, supervised=supervised, url_list=chunk)
        all_scraped_list+=scraped_list
        all_missed_list+=missed_list
        
    if i != len(chunked_list)-1:
        for ii in range(len(chunked_list) -1 - i):
            all_missed_list+=chunked_list[i+ii+1]
    print(f"Super scrape complete. Missed: {len(all_missed_list)}")
    return all_scraped_list, all_missed_list
