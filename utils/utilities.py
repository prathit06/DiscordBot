import logging
import sys
import os
import psycopg2
from dotenv import load_dotenv
import math
import pandas as pd
from unidecode import unidecode

import discord

logger = logging.getLogger()
logger.setLevel(logging.INFO)

load_dotenv()
postgre_conn_uri = os.environ["DATABASE_URL"]


async def get_normal_wars_embed():
    try:
        con = psycopg2.connect(postgre_conn_uri)
        cur = con.cursor()

        cur.execute("""
        select player_name,season,totalStars,totalDestruction,
        warsplayed as Wars from (
        select player_name,season,sum(stars) totalStars,sum(destruction) totalDestruction,count(distinct startTime) as warsplayed
        from normal_war_attacks
        where season = (select max(season) from normal_war_attacks)
        group by player_name,season
        order by totalStars desc,totalDestruction desc ) as A
        order by totalStars desc;
        """)

        rows = cur.fetchall()
        total_pages = math.ceil(len(rows)/10)

        pages = []

        for page in range(1, total_pages+1):
            lstname = []
            lststars = []
            lstdest = []
            lstavg_stars = []

            df = pd.DataFrame()

            embed = discord.Embed(title="__**Clan Wars Leader Board**__", color=discord.Color.blue())
            for index, row in enumerate(rows):
                if index < 10*page and index >= 10*page-10:
                    lstname.append(unidecode(row[0]))
                    lststars.append(row[2])
                    lstdest.append(row[3])
                    lstavg_stars.append(row[4])

            df['PlayerName'] = lstname
            df['Stars'] = lststars
            df['Dest'] = lstdest
            df['Wars'] = lstavg_stars
            df = df.to_markdown(index=False)

            embed.add_field(name="Info", value="`{}`".format(df), inline=True)
            embed.set_footer(text="Season : {season} • Made by BeoWulf • Page {pagenum}/{totalpages}".format(
                season=row[1], pagenum=page, totalpages=total_pages))
            pages.append(embed)

        con.commit()
        con.close()

    except Exception as e:
        logging.error("Error {}:".format(e))
        sys.exit(1)
    finally:
        if con:
            con.close()
    return pages


async def insertRecordsInDb_normal_wars(clantag, client):

    try:

        latest_season = await client.get_seasons("29000022")
        latest_season = latest_season[-1]['id']

        insertRecords = {}
        row_count = 0

        war_info = await client.get_clan_war('#92YQU2C')
        war_type = war_info.type

        if war_type == "random":
            logging.info("Random war ongoing")
            for attack in war_info.attacks:
                player_info = await client.get_player(attack.attacker_tag)
                if player_info.clan is None:
                    pass
                elif(player_info.clan.name == 'IMAGINE DRAGONS'):
                    try:
                        insertRecords['season'] = latest_season
                    except Exception as e:
                        logging.error("Exception in season :", e)
                        insertRecords['season'] = " "
                    try:
                        insertRecords['startTime'] = str(war_info.start_time.time)
                    except Exception as e:
                        logging.error("Exception in startTime :", e)
                        insertRecords['startTime'] = " "
                    try:
                        insertRecords['endTime'] = str(war_info.end_time.time)
                    except Exception as e:
                        logging.error("Exception in endTime :", e)
                        insertRecords['endTime'] = " "
                    try:
                        insertRecords['player_name'] = player_info.name
                    except Exception as e:
                        logging.error("Exception in player_name :", e)
                        insertRecords['player_name'] = " "
                    try:
                        insertRecords['player_townhalllevel'] = player_info.town_hall
                    except Exception as e:
                        logging.error("Exception in player_townhalllevel :", e)
                        insertRecords['player_townhalllevel'] = " "
                    try:
                        insertRecords['stars'] = attack.stars
                    except Exception as e:
                        logging.error("Exception in stars :", e)
                        insertRecords['stars'] = " "
                    try:
                        insertRecords['destruction'] = attack.destruction
                    except Exception as e:
                        logging.error("Exception in destruction :", e)
                        insertRecords['destruction'] = " "
                    try:
                        insertRecords['defender_tag'] = attack.defender_tag
                    except Exception as e:
                        logging.error("Exception in defender_tag :", e)
                        insertRecords['defender_tag'] = " "

                    try:
                        con = psycopg2.connect(postgre_conn_uri)
                        cur = con.cursor()
                        query = """
                        insert into normal_war_attacks(""" + ','.join(list(insertRecords.keys()))+""")
                        values  """ + str(tuple(insertRecords.values())) + """
                        on conflict(season,startTime,player_name,stars,destruction,defender_tag)
                        do nothing
                        """
                        cur.execute(query)

                        row_count = row_count + cur.rowcount
                        con.commit()
                        con.close()

                    except Exception as e:
                        logging.error("Error {}:".format(e))
                        sys.exit(1)
                    finally:
                        if con:
                            con.close()
        else:
            logging.info("Type of War is not random but {}".format(war_type))

        logging.info("upserted {} rows in normal_war_attacks table !".format(row_count))
    except Exception as e:
        logging.error("Error occured in insertRecordsInDb_normal_wars() fun :", e)


async def insertRecordsInDb_CWL(clantag, client):

    war = await client.get_clan_war('#92YQU2C')

    if war.is_cwl:
        try:

            latest_season = await client.get_seasons("29000022")
            latest_season = latest_season[-1]['id']

            group = await client.get_league_group('#92YQU2C')
            insertRecords = {}
            row_count = 0

            async for war in group.get_wars_for_clan('#92YQU2C'):
                for attack in war.attacks:
                    player_info = await client.get_player(attack.attacker_tag)
                    if player_info.clan is None:
                        pass
                    elif(player_info.clan.name == 'IMAGINE DRAGONS'):
                        try:
                            insertRecords['season'] = latest_season
                        except Exception as e:
                            logging.error("Exception in season :", e)
                            insertRecords['season'] = " "
                        try:
                            insertRecords['startTime'] = war.start_time.time
                        except Exception as e:
                            logging.error("Exception in startTime :", e)
                            insertRecords['startTime'] = " "
                        try:
                            insertRecords['endTime'] = war.end_time.time
                        except Exception as e:
                            logging.error("Exception in endTime :", e)
                            insertRecords['endTime'] = " "
                        try:
                            insertRecords['player_name'] = player_info.name
                        except Exception as e:
                            logging.error("Exception in player_name :", e)
                            insertRecords['player_name'] = " "
                        try:
                            insertRecords['player_townhalllevel'] = player_info.town_hall
                        except Exception as e:
                            logging.error("Exception in player_townhalllevel :", e)
                            insertRecords['player_townhalllevel'] = " "
                        try:
                            insertRecords['stars'] = attack.stars
                        except Exception as e:
                            logging.error("Exception in stars :", e)
                            insertRecords['stars'] = " "
                        try:
                            insertRecords['destruction'] = attack.destruction
                        except Exception as e:
                            logging.error("Exception in destruction :", e)
                            insertRecords['destruction'] = " "

                        try:
                            con = psycopg2.connect(postgre_conn_uri)
                            cur = con.cursor()

                            query = """
                            insert into normal_war_attacks(""" + ','.join(list(insertRecords.keys()))+""")
                            values  """ + str(tuple(insertRecords.values())) + """
                            on conflict(season,startTime,player_name,stars,destruction)
                            do nothing
                            """
                            cur.execute(query)

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
            logging.error("Error occured in insertRecordsInDb_CWL() fun:", e)
    else:
        logging.info("No cwl currently")
