import os
from config import configurations
import requests
import logging
import json
import pandas as pd
import sqlite3 as lite
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def api_calls_get_template(coc_url):
    COC_TOKEN = os.getenv('COC_TOKEN')
    headers = {
        'authorization': f'Bearer {COC_TOKEN}',
        'Accept': 'application/json'
    }
    response = requests.get(coc_url, headers=headers)
    return response.json()


def fetchPlayerTrophies(playertag):
    request_url = configurations.config_dict[
        'coc_api_url_fetch_player_info'].format(playertag=playertag.replace("#", ""))

    response = api_calls_get_template(request_url)
    return response['bestTrophies']


def getClanMembers(clantag):
    request_url = configurations.config_dict[
        'coc_api_url_fetch_clan_members'].format(clantag=clantag.replace("#", ""))
    response = api_calls_get_template(request_url)
    members_resp = json.dumps(response)
    return members_resp


def getWarInfo(clantag):
    request_url = configurations.config_dict[
        'coc_api_url_fetch_current_war'].format(clantag=clantag.replace("#", ""))
    response = api_calls_get_template(request_url)
    members_resp = json.dumps(response)
    return members_resp


def getLatestSeasonInfo():
    request_url = configurations.config_dict['coc_api_url_fetch_seasons_details']
    response = api_calls_get_template(request_url)
    json_resp = json.dumps(response)
    # logging.info(json_resp)
    return json_resp


def insertRecordsInDb(clantag):

    main_keys = ['teamSize', 'startTime', 'endTime', 'clan_tag', 'clan_name']
    row_count = 0

    latest_season = json.loads(getLatestSeasonInfo())['items'][-1]['id']

    warinfo = getWarInfo(clantag)
    warinfo_dict = json.loads(warinfo)

    df = pd.json_normalize(warinfo_dict, sep='_')
    flatten_dict = df.to_dict(orient='records')[0]

    main_dict = {}
    members_list = pd.json_normalize(flatten_dict['clan_members'], sep='_').to_dict(orient='records')

    for key, value in flatten_dict.items():
        if key in main_keys:
            main_dict[key] = value

    for member in members_list:

        member_attack_info = {}
        member_attack_info['season'] = latest_season
        member_attack_info['player_tag'] = member['tag']
        member_attack_info['player_name'] = member['name']
        member_attack_info['player_townhalllevel'] = member['townhallLevel']

        try:
            first_attack_stars = member['attacks'][0]['stars']
            first_attack_destructionPercentage = member['attacks'][0]['destructionPercentage']
        except:
            first_attack_stars = 0
            first_attack_destructionPercentage = 0
        try:
            second_attack_stars = member['attacks'][1]['stars']
            second_attack_destructionPercentage = member['attacks'][1]['destructionPercentage']
        except:
            second_attack_stars = 0
            second_attack_destructionPercentage = 0

        member_attack_info['stars'] = first_attack_stars + second_attack_stars
        member_attack_info['destruction'] = first_attack_destructionPercentage + second_attack_destructionPercentage

        merged_dict = {**main_dict, **member_attack_info}

        try:
            con = lite.connect('test.db')
            cur = con.cursor()

            cur.execute("""
            INSERT OR REPLACE INTO war_attacks(teamSize, startTime, endTime, clan_tag, clan_name, season, player_tag, player_name, player_townhalllevel, stars, destruction)
            VALUES (:teamSize, :startTime, :endTime, :clan_tag, :clan_name, :season, :player_tag, :player_name, :player_townhalllevel, :stars, :destruction)
            on conflict (season,startTime,player_tag) do
            update set stars = {}, destruction = {}, update_timestamp = CURRENT_TIMESTAMP
            """.format(merged_dict['stars'], merged_dict['destruction']), merged_dict)

            row_count = row_count + cur.rowcount
            con.commit()
            con.close()

        except Exception as e:
            logging.error("Error {}:".format(e))
            sys.exit(1)
        finally:
            if con:
                con.close()

    logging.info("upserted {} rows !".format(row_count))
