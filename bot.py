import os
import logging
from dotenv import load_dotenv
import sys
import psycopg2
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
postgre_conn_uri = os.environ["DATABASE_URL"] 


@bot.event
async def on_ready():
    logging.info(f'{bot.user.name} is connected to Discord !')
    try:
        con = psycopg2.connect(postgre_conn_uri)
        cur = con.cursor()
        sql_script_path = 'create_table.sql'

        with open(sql_script_path, 'r') as sql_file:
            sql_script = sql_file.read()

        cur.execute(sql_script)
        logging.info('table created')
        con.commit()
        con.close()

    except Exception as e:
        logging.error("Error connecting to db in on_ready() fun {}:".format(e))
        sys.exit(1)
    finally:
        if con:
            con.close()


@bot.command(name='ping', help='checks if the bot is live')
async def pingBot(ctx):
    logging.info("in ping command")
    await ctx.send('Bot up and running')


@tasks.loop(seconds=60)
async def loop():
    try:
        con = psycopg2.connect(postgre_conn_uri)
        cur = con.cursor()
        sql_script_path = 'create_table.sql'

        with open(sql_script_path, 'r') as sql_file:
            sql_script = sql_file.read()

        cur.execute(sql_script)
        logging.info('table created')
        con.commit()
        con.close()

    except Exception as e:
        logging.error("Error connecting to db {}:".format(e))
        sys.exit(1)
    finally:
        if con:
            con.close()

    try:
        await utilities.insertRecordsInDb_CWL("#92YQU2C", client)
        logging.info("Refreshed cwl table")
    except Exception as e:
        logging.exception("Exception occured in loop() for insertRecordsInDb_CWL : {}".format(e))
    try:
        await utilities.insertRecordsInDb_normal_wars("#92YQU2C", client)
        logging.info("Refreshed normal wars table")
    except Exception as e:
        logging.exception("Exception occured in loop() for insertRecordsInDb_normal_wars : {}".format(e))


@bot.command(name='warinfo', help='fetches normal wars info')
async def getClanWarInfo(ctx):

    embeds = await utilities.get_normal_wars_embed()

    embed_index = 0
    message = await ctx.send(embed=embeds[embed_index])
    await message.add_reaction('⬆️')
    await message.add_reaction('⬇️')
    await message.add_reaction('🔄')

    reactable = True

    def check_reaction(r, user):
        allowed_list = ['⬆️', '⬇️', '🔄']
        return r.message == message and str(r.emoji) in allowed_list and not user.bot

    while reactable:
        try:
            reaction, user = await bot.wait_for('reaction_add', check=check_reaction)
            react_str = str(reaction.emoji)
            if react_str == '⬆️':
                embed_index += 1
            elif react_str == '⬇️':
                embed_index -= 1
            elif react_str == '🔄':
                embeds = await utilities.get_normal_wars_embed()
                await message.edit(embed=embeds[0])

            await message.edit(embed=embeds[embed_index % len(embeds)])
            await reaction.remove(user)
        except TimeoutError:
            await message.clear_reactions()
            reactable = False


# @bot.command(name='cwlinfo', help='fetches cwl info')
# async def getCWLWarInfo(ctx):

#     try:
#         con = psycopg2.connect(postgre_conn_uri)
#         cur = con.cursor()

#         cur.execute("""
#         select player_name,season,totalStars,totalDestruction,
#         round(cast(totalStars as decimal)/cast(warsplayed as decimal),3) as avg_stars from (
#         select player_name,season,sum(stars) totalStars,sum(destruction) totalDestruction,count(1) as warsplayed
#         from normal_war_attacks
#         where season = (select max(season) from normal_war_attacks)
#         group by player_name,season
#         order by totalStars desc,totalDestruction desc ) as A
#         order by avg_stars desc;
#         """)

#         rows = cur.fetchall()
#         total_pages = math.ceil(len(rows)/5)

#         pages = []

#         for page in range(1, total_pages+1):
#             lstname = []
#             lststars = []
#             lstdest = []
#             lstavg_stars = []

#             df = pd.DataFrame()

#             embed = discord.Embed(title="__**CWL Leader Board**__", color=discord.Color.blue())
#             for index, row in enumerate(rows):
#                 if index < 5*page and index >= 5*page-5:
#                     lstname.append(unidecode(row[0]))
#                     lststars.append(row[2])
#                     lstdest.append(row[3])
#                     lstavg_stars.append(row[4])

#             df['Player Name'] = lstname
#             df['Stars'] = lststars
#             df['Destruction'] = lstdest
#             df['Avg Stars'] = lstavg_stars
#             df = df.to_markdown(index=False)

#             embed.add_field(name="Info", value="`{}`".format(df), inline=True)
#             embed.set_footer(text="Season : {} • Made by BeoWulf".format(row[1]))
#             pages.append(embed)

#         con.commit()
#         con.close()

#     except Exception as e:
#         logging.error("Error {}:".format(e))
#         sys.exit(1)
#     finally:
#         if con:
#             con.close()

#     paginator = Paginator(pages=pages, timeout=30000000.0, compact=True)
#     await paginator.start(ctx)

loop.start()
bot.run(DISCORD_TOKEN)
