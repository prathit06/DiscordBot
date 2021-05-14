import os
import logging
from dotenv import load_dotenv
import sys
import sqlite3 as lite
import pandas as pd
from unidecode import unidecode
import math

import coc

import discord
from discord.ext import commands, tasks
from pygicord import Paginator

from utils import utilities

load_dotenv()
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')

BOT_PREFIX = "id!"

bot = commands.Bot(command_prefix=BOT_PREFIX)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dir_path = os.path.dirname(os.path.realpath(__file__))

con = None

client = coc.login(os.environ["DEV_SITE_EMAIL"], os.environ["DEV_SITE_PASSWORD"])


@bot.event
async def on_ready():
    logging.info(f'{bot.user.name} is connected to Discord !')
    try:
        con = lite.connect('test.db')
        cur = con.cursor()
        sql_script_path = 'create_table.sql'

        with open(sql_script_path, 'r') as sql_file:
            sql_script = sql_file.read()

        cur.executescript(sql_script)
        logging.info('table created')
        con.commit()
        con.close()

    except Exception as e:
        logging.error("Error occured in on_ready() fun {}:".format(e))
        sys.exit(1)
    finally:
        if con:
            con.close()


@bot.command(name='ping', help='checks if the bot is live')
async def pingBot(ctx):
    logging.info("in ping command")
    await ctx.send('Bot up and running')


@tasks.loop(seconds=30)
async def loop():
    logging.info("Refrshing DB")
    try:
        await utilities.insertRecordsInDb_CWL("#92YQU2C", client)
    except Exception as e:
        logging.exception("Exception occured in loop() for insertRecordsInDb_CWL : {}".format(e))
    try:
        await utilities.insertRecordsInDb_normal_wars("#92YQU2C", client)
    except Exception as e:
        logging.exception("Exception occured in loop() for insertRecordsInDb_normal_wars : {}".format(e))


@bot.command(name='warinfo', help='fetches normal wars info')
async def getClanWarInfo(ctx):

    try:
        con = lite.connect('test.db')
        cur = con.cursor()

        cur.execute("""
        select player_name,season,totalStars,totalDestruction,
        round(cast(totalStars as float)/cast(warsplayed as float),3) as avg_stars from (
        select player_name,season,sum(stars) totalStars,sum(destruction) totalDestruction,count(1) as warsplayed
        from normal_war_attacks
        where season = (select max(season) from normal_war_attacks)
        group by player_name,season
        order by totalStars desc,totalDestruction desc )
        order by avg_stars desc;
        """)

        rows = cur.fetchall()
        total_pages = math.ceil(len(rows)/5)

        pages = []

        for page in range(1, total_pages+1):
            lstname = []
            lststars = []
            lstdest = []
            lstavg_stars = []

            df = pd.DataFrame()

            embed = discord.Embed(title="__**Clan Wars Leader Board**__", color=discord.Color.blue())
            for index, row in enumerate(rows):
                if index < 5*page and index >= 5*page-5:
                    lstname.append(unidecode(row[0]))
                    lststars.append(row[2])
                    lstdest.append(row[3])
                    lstavg_stars.append(row[4])

            df['PlayerName'] = lstname
            df['Stars'] = lststars
            df['Dest'] = lstdest
            df['AvgStars'] = lstavg_stars
            df = df.to_markdown(index=False)

            # print(df)

            embed.add_field(name=" ", value="`{}`".format(df), inline=True)
            embed.set_footer(text="Season : {} • Made by BeoWulf".format(row[1]))
            pages.append(embed)

        con.commit()
        con.close()

    except Exception as e:
        logging.error("Error {}:".format(e))
        sys.exit(1)
    finally:
        if con:
            con.close()

    paginator = Paginator(pages=pages, timeout=30000000.0, compact=True)
    await paginator.start(ctx)


@bot.command(name='cwlinfo', help='fetches cwl info')
async def getCWLWarInfo(ctx):

    try:
        con = lite.connect('test.db')
        cur = con.cursor()

        cur.execute("""
        select player_name,season,totalStars,totalDestruction,
        round(cast(totalStars as float)/cast(warsplayed as float),3) as avg_stars from (
        select player_name,season,sum(stars) totalStars,sum(destruction) totalDestruction,count(1) as warsplayed
        from cwl_war_attacks
        where season = (select max(season) from cwl_war_attacks)
        group by player_name,season
        order by totalStars desc,totalDestruction desc )
        order by avg_stars desc;
        """)

        rows = cur.fetchall()
        # print(rows)
        total_pages = int(len(rows)/5)

        pages = []

        for page in range(1, total_pages+1):
            lstname = []
            lststars = []
            lstdest = []
            lstavg_stars = []

            df = pd.DataFrame()

            embed = discord.Embed(title="__**CWL Leader Board**__", color=discord.Color.blue())
            for index, row in enumerate(rows):
                if index <= 5*page and index >= 5*page-5:
                    lstname.append(unidecode(row[0]))
                    lststars.append(row[2])
                    lstdest.append(row[3])
                    lstavg_stars.append(row[4])

            df['Player Name'] = lstname
            df['Stars'] = lststars
            df['Destruction'] = lstdest
            df['Avg Stars'] = lstavg_stars
            df = df.to_markdown(index=False)

            # print(df)

            embed.add_field(name="details", value="`{}`".format(df), inline=True)
            embed.set_footer(text="Season : {} • Made by BeoWulf".format(row[1]))
            pages.append(embed)

        con.commit()
        con.close()

    except Exception as e:
        logging.error("Error {}:".format(e))
        sys.exit(1)
    finally:
        if con:
            con.close()

    paginator = Paginator(pages=pages, timeout=30000000.0, compact=True)
    await paginator.start(ctx)

loop.start()
bot.run(DISCORD_TOKEN)
