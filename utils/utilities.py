import logging
import sqlite3 as lite
import sys

logger = logging.getLogger()
logger.setLevel(logging.INFO)


async def insertRecordsInDb_normal_wars(clantag, client):

    try:

        latest_season = await client.get_seasons("29000022")
        latest_season = latest_season[-1]['id']

        # group = await client.get_league_group('#92YQU2C')
        insertRecords = {}
        row_count = 0

        war_info = await client.get_clan_war()
        for attack in war_info.attacks:
            player_info = await client.get_player(attack.attacker_tag)
            if(player_info.clan.name == 'IMAGINE DRAGONS'):
                try:
                    insertRecords['season'] = latest_season
                except:
                    insertRecords['season'] = " "
                try:
                    insertRecords['startTime'] = war.start_time.time
                except:
                    insertRecords['startTime'] = " "
                try:
                    insertRecords['endTime'] = war.end_time.time
                except:
                    insertRecords['endTime'] = " "
                try:
                    insertRecords['player_name'] = player_info.name
                except:
                    insertRecords['player_name'] = " "
                try:
                    insertRecords['player_townhalllevel'] = player_info.town_hall
                except:
                    insertRecords['player_townhalllevel'] = " "
                try:
                    insertRecords['stars'] = attack.stars
                except:
                    insertRecords['stars'] = " "
                try:
                    insertRecords['destruction'] = attack.destruction
                except:
                    insertRecords['destruction'] = " "

                try:
                    con = lite.connect('test.db')
                    cur = con.cursor()

                    cur.execute("""
                    INSERT OR REPLACE INTO cwl_war_attacks(startTime, endTime, season, player_name, player_townhalllevel, stars, destruction)
                    VALUES (:startTime, :endTime, :season, :player_name, :player_townhalllevel, :stars, :destruction)
                    on conflict (season,startTime,player_name) do
                    update set stars = {}, destruction = {}, update_timestamp = CURRENT_TIMESTAMP
                    """.format(insertRecords['stars'], insertRecords['destruction']), insertRecords)

                    row_count = row_count + cur.rowcount
                    con.commit()
                    con.close()

                except Exception as e:
                    logging.error("Error {}:".format(e))
                    sys.exit(1)
                finally:
                    if con:
                        con.close()

        logging.info("upserted {} rows in cwl_war_attacks table !".format(row_count))
    except:
        logging.error("No CWL league as of now")


async def insertRecordsInDb_CWL(clantag, client):

    try:

        latest_season = await client.get_seasons("29000022")
        latest_season = latest_season[-1]['id']

        group = await client.get_league_group('#92YQU2C')
        insertRecords = {}
        row_count = 0

        async for war in group.get_wars_for_clan('#92YQU2C'):
            # if(war.clan_tag == '#92YQU2C'):
            for attack in war.attacks:
                player_info = await client.get_player(attack.attacker_tag)
                if(player_info.clan.name == 'IMAGINE DRAGONS'):
                    try:
                        insertRecords['season'] = latest_season
                    except:
                        insertRecords['season'] = " "
                    try:
                        insertRecords['startTime'] = war.start_time.time
                    except:
                        insertRecords['startTime'] = " "
                    try:
                        insertRecords['endTime'] = war.end_time.time
                    except:
                        insertRecords['endTime'] = " "
                    try:
                        insertRecords['player_name'] = player_info.name
                    except:
                        insertRecords['player_name'] = " "
                    try:
                        insertRecords['player_townhalllevel'] = player_info.town_hall
                    except:
                        insertRecords['player_townhalllevel'] = " "
                    try:
                        insertRecords['stars'] = attack.stars
                    except:
                        insertRecords['stars'] = " "
                    try:
                        insertRecords['destruction'] = attack.destruction
                    except:
                        insertRecords['destruction'] = " "

                    try:
                        con = lite.connect('test.db')
                        cur = con.cursor()

                        cur.execute("""
                        INSERT OR REPLACE INTO cwl_war_attacks(startTime, endTime, season, player_name, player_townhalllevel, stars, destruction)
                        VALUES (:startTime, :endTime, :season, :player_name, :player_townhalllevel, :stars, :destruction)
                        on conflict (season,startTime,player_name) do
                        update set stars = {}, destruction = {}, update_timestamp = CURRENT_TIMESTAMP
                        """.format(insertRecords['stars'], insertRecords['destruction']), insertRecords)

                        row_count = row_count + cur.rowcount
                        con.commit()
                        con.close()

                    except Exception as e:
                        logging.error("Error {}:".format(e))
                        sys.exit(1)
                    finally:
                        if con:
                            con.close()

        logging.info("upserted {} rows in cwl_war_attacks table !".format(row_count))
    except Exception as e:
        logging.error("Error occured :", e)
