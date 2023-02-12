import logging

from aiogram import Bot, Dispatcher, executor, types
from config import token, user_id
import asyncio
import pandas as pd
from dispatch_bot import *
from rpm_calc3 import send_new_notifications
from aiogram.utils.markdown import hbold

# Configure logging
logging.basicConfig(level=logging.INFO)

input_text = "Chattanooga, TN, 37421"
lat, lon = 35.3813808,-81.3835815
canada = ['AB','BC','MB','NB','NL','NS','ON','PE','QC','SK','Ca']
########################################################################################################

bot = Bot(token=token, parse_mode=types.ParseMode.HTML)
dp = Dispatcher(bot)


async def new_loads_notification():
    while True:
        df_l = pd.read_csv('csv_data/live.csv').drop_duplicates(subset=['Pickup City', 'Vehicle Info', 'Delivery City']).drop_duplicates(subset=['Pickup City', 'Vehicle Info', 'Company Name']).drop_duplicates(subset=['Company Name', 'Vehicle Info', 'Delivery City']).reset_index(drop=True)
        df = df_l.query('`Pickup State` not in @canada and `Delivery State` not in @canada').copy()
        df = df[df['Distance'] > 0]
        df['RPM'] = round(df['Rate']/df['Distance'], 2)
        df = df[~df['Vehicle Info'].str.contains(r'[4-9]\sunit|1[0-5]\sunit', regex=True)]
        df1 = df.query('`Vehicle Type` in ["MOTORCYCLE", "ATV"] and RPM >= 0.4 and Rate >= 300').copy()
        df2 = df.query('`Vehicle Type` in ["CAR", "SUV", "PICKUP"] and RPM >= 1 and Rate >= 450').copy()
        truck = (lat, lon)

        # For pairs
        df_n = await nearest_coordinates(df1, truck, n_neighbors=30)
        df_n2 = await nearest_coordinates(df2, truck, n_neighbors=40)
        # For triplets
        df_n_db3 = await nearest_coordinates(df1, truck, n_neighbors=18)
        df_n2_db3 = await nearest_coordinates(df2, truck, n_neighbors=25)

        df_db1 = await find_veh_db1(df_n, truck, filter_by=1)
        df_db2 = await find_veh_db2(df_n, df_n2, truck, filter_by=1.55)
        df_db3 = await find_veh_db3(df_n_db3, df_n_db3, df_n2_db3, truck, filter_by=1.75)

        df_n_n_db1 = await send_new_notifications(df_db1, 'nn_sent_db1')
        df_n_n_db2 = await send_new_notifications(df_db2, 'nn_sent_db2')
        df_n_n_db3 = await send_new_notifications(df_db3, 'nn_sent_db3')

        if not df_n_n_db1.empty:
            await bot.send_message(user_id, hbold('Notifications  ➡️  ➡️   ➡️  📩  ⬅️  ⬅️   ⬅️'))
            await card_sender(user_id, df_n_n_db1, input_text)
            await bot.send_message(user_id, hbold('Notifications  ➡️  ➡️   ➡️  📩  ⬅️  ⬅️   ⬅️'))
        if not df_n_n_db2.empty:
            await bot.send_message(user_id, hbold('Notifications  ➡️  ➡️   ➡️  📩  ⬅️  ⬅️   ⬅️'))
            await card_sender(user_id, df_n_n_db2, input_text)
            await bot.send_message(user_id, hbold('Notifications  ➡️  ➡️   ➡️  📩  ⬅️  ⬅️   ⬅️'))
        if not df_n_n_db3.empty:
            await bot.send_message(user_id, hbold('Notifications  ➡️  ➡️   ➡️  📩  ⬅️  ⬅️   ⬅️'))
            await card_sender_db3(user_id, df_n_n_db3, input_text)
            await bot.send_message(user_id, hbold('Notifications  ➡️  ➡️   ➡️  📩  ⬅️  ⬅️   ⬅️'))
        
        await asyncio.sleep(120)

if __name__ == "__main__":
    try:
        asyncio.run(new_loads_notification())
    except KeyboardInterrupt:
        pass
    executor.start_polling(dp)