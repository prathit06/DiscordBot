import os
import logging
from dotenv import load_dotenv
import json
import sys
import sqlite3 as lite
import pandas as pd
from unidecode import unidecode

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


@bot.event
async def on_ready():
    logging.info(f'{bot.user.name} is connected to Discord !')
    try:
        con = lite.connect('test.db')
        cur = con.cursor()
        sql_script_path = dir_path+'\sql_utils\create_table.sql'

        with open(sql_script_path, 'r') as sql_file:
            sql_script = sql_file.read()

        cur.executescript(sql_script)
        logging.info('table created')
        con.commit()
        con.close()

    except Exception as e:
        logging.error("Error {}:".format(e))
        sys.exit(1)
    finally:
        if con:
            con.close()


@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.errors.CheckFailure):
        logging.info(error)
        await ctx.send('You do not have the correct role for this command.')


@bot.command(name='ping', help='checks if the bot is live')
async def pingBot(ctx):
    logging.info("in ping command")
    await ctx.send('Bot up and running')


@bot.command(name='alltimebest', help='fetches player info')
async def getPlayerInfo(ctx, playertag):
    bestTrophies = utilities.fetchPlayerTrophies(playertag)
    await ctx.send('Your best trophies were ' + str(bestTrophies))


@bot.command(name='clanmembers', help='fetches clan members')
async def getClanMembers(ctx, clantag):

    members = utilities.getClanMembers(clantag)
    members_dict = json.loads(members)["items"]

    embed = discord.Embed(title="Clan Members Info", color=discord.Color.blue())
    embed.set_author(name="WarLeaderboardBot", icon_url="http://commondatastorage.googleapis.com/codeskulptor-assets/week5-triangle.png")

    for index_member, member in enumerate(members_dict):
        if index_member < 5:
            embed.add_field(name="Name", value=member['name'], inline=True)
            embed.add_field(name="Tag", value=member['tag'], inline=True)
            embed.add_field(name="Trophies", value=member['trophies'], inline=True)

    await ctx.send(embed=embed)


@tasks.loop(seconds=30)
async def loop():
    logging.info("Refrshing DB")
    utilities.insertRecordsInDb("#92YQU2C")


@bot.command(name='warinfo', help='fetches war info')
async def getWarInfo(ctx):

    try:
        con = lite.connect('test.db')
        cur = con.cursor()

        cur.execute("""
        select player_name,season,sum(stars) totalStars,sum(destruction) totalDestruction
        from war_attacks
        where season = (select max(season) from war_attacks)
        group by player_name,season
        order by totalStars desc,totalDestruction desc ;
        """)

        rows = cur.fetchall()
        total_pages = int(len(rows)/5)

        pages = []

        for page in range(1, total_pages+1):
            lstname = []
            lststars = []
            lstdest = []

            df = pd.DataFrame()

            embed = discord.Embed(title="__**War Leader Board**__", color=discord.Color.blue())
            # embed.set_thumbnail(url="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSxNDTGrePgl5cMkXr48xEeKeLs43-z6oq1EA&usqp=CAU")
            for index, row in enumerate(rows):
                if index < 5*page and index >= 5*page-5:
                    # print(row[0])
                    lstname.append(unidecode(row[0]))
                    # lstname.append(unicodedata.normalize('NFKD', row[0]).encode('ascii', 'ignore').decode("utf-8"))
                    lststars.append(row[2])
                    lstdest.append(row[3])

            df['Name'] = lstname
            df['stars'] = lststars
            df['dest'] = lstdest
            df = df.to_markdown(index=False)

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
