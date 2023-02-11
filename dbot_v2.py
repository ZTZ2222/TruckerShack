import logging

from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from config import token
import pandas as pd
from geopy.distance import geodesic
from sklearn.neighbors import BallTree
import numpy as np
import asyncio
import aiohttp
from aiogram.utils.markdown import hbold, hitalic, hcode, hlink
import datetime
import itertools
import networkx as nx


API_TOKEN = token

conversation_state = {}

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize bot and dispatcher
bot = Bot(API_TOKEN, parse_mode=types.ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

canada = ['AB','BC','MB','NB','NL','NS','ON','PE','QC','SK','Ca']


@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    chat_id = message.from_user.id
    # Setup keyboard
    markup = types.inline_keyboard.InlineKeyboardMarkup()
    btn1 = types.inline_keyboard.InlineKeyboardButton("2 cars 🚗🚙", callback_data="1")
    btn2 = types.inline_keyboard.InlineKeyboardButton("2 motos 🛵🏍", callback_data="2")
    btn3 = types.inline_keyboard.InlineKeyboardButton("moto & car 🏍🚗", callback_data="3")
    btn4 = types.inline_keyboard.InlineKeyboardButton("2 moto & car 🛵🏍🚗", callback_data="4")
    btn5 = types.inline_keyboard.InlineKeyboardButton("3 moto 🏍🏍🏍", callback_data="5")
    markup.add(btn1, btn2, btn3, btn4, btn5)
    
    await message.answer("Greetings! I am here to assist you in finding loads on Central Dispatch. May I have some information to start with? Please select an option to specify your needs:", reply_markup=markup)

@dp.callback_query_handler(lambda c: c.data in ["1", "2", "3", "4", "5"])
async def process_callback_option(callback_query: types.CallbackQuery):
    chat_id = callback_query.from_user.id
    conversation_state[chat_id] = callback_query.data

    options = {"1": "🚗🚙", "2": "🛵🏍", "3": "🏍🚗", "4": "🛵🏍🚗", "5": "🏍🏍🏍"}
    selected_option = options[callback_query.data]
    await bot.answer_callback_query(callback_query.id, text=f"Option {selected_option} selected.")

    await bot.send_message(chat_id=callback_query.from_user.id, text="Please input Truck's location: City, State, ZIP-code separated by commas. \ne.g. Atlanta, GA or Chattanooga, TN, 37421")


# Save user's text and call functions
@dp.message_handler(lambda message: message.text)
async def process_text_message(message: types.Message):
    chat_id = message.from_user.id
    option = conversation_state.get(chat_id)
    if option is None:
        # The user hasn't clicked any option yet
        return
    input_text = message.text
    split_text = input_text.split(',')
    if len(split_text) < 2:
        await message.answer("Sorry, the location provided is incorrect. Please enter valid location to continue:")
        return
    city = split_text[0].strip()
    state = split_text[1].strip()
    zip_code = None
    if len(split_text) > 2:
        zip_code = split_text[2].strip()
    lat, lon, full_name = await city_state_to_coordinates(city, state, zip_code)
    if lat == None or lon == None:
        await message.answer("Sorry, the location provided is incorrect. Please enter valid location to continue:")
        return
    await bot.send_message(chat_id=chat_id, text=f"Searching... \n{full_name}")
    truck = (lat, lon)

    df_l = pd.read_csv('csv_data/live.csv').drop_duplicates(subset=['Pickup City', 'Vehicle Info', 'Delivery City']).drop_duplicates(subset=['Pickup City', 'Vehicle Info', 'Company Name']).drop_duplicates(subset=['Company Name', 'Vehicle Info', 'Delivery City']).reset_index(drop=True)
    df = df_l.query('`Pickup State` not in @canada and `Delivery State` not in @canada').copy()
    df = df[df['Distance'] > 0]
    df['RPM'] = round(df['Rate']/df['Distance'], 2)
    df = df[~df['Vehicle Info'].str.contains(r'[4-9]\sunit|1[0-5]\sunit', regex=True)]
    

    if option == "1":
        df2 = df.query('`Vehicle Type` in ["CAR", "SUV", "PICKUP"] and RPM >= 0.8 and Rate >= 450').copy()
        df_n = await nearest_coordinates(df2, truck, n_neighbors=50)
        df_db1 = await find_veh_db1(df_n, truck, filter_by=1.8)
        await bot.send_message(chat_id=chat_id, text=f"I was able to locate {df_db1.shape[0]} itineraries.")
        await card_sender(chat_id, df_db1, input_text)
    elif option == "2":
        df1 = df.query('`Vehicle Type` in ["MOTORCYCLE", "ATV"] and RPM >= 0.35 and Rate >= 300').copy()
        df_n = await nearest_coordinates(df1, truck, n_neighbors=40)
        df_db1 = await find_veh_db1(df_n, truck, filter_by=0.9)
        await bot.send_message(chat_id=chat_id, text=f"I was able to locate {df_db1.shape[0]} itineraries.")
        await card_sender(chat_id, df_db1, input_text)
    elif option == "3":
        df1 = df.query('`Vehicle Type` in ["MOTORCYCLE", "ATV"] and RPM >= 0.35 and Rate >= 300').copy()
        df2 = df.query('`Vehicle Type` in ["CAR", "SUV", "PICKUP"] and RPM >= 0.9 and Rate >= 450').copy()
        df_n = await nearest_coordinates(df1, truck, n_neighbors=40)
        df_n2 = await nearest_coordinates(df2, truck, n_neighbors=50)
        df_db2 = await find_veh_db2(df_n, df_n2, truck, filter_by=1.55)
        await bot.send_message(chat_id=chat_id, text=f"I was able to locate {df_db2.shape[0]} itineraries.")
        await card_sender(chat_id, df_db2, input_text)
    elif option == "4":
        df1 = df.query('`Vehicle Type` in ["MOTORCYCLE", "ATV"] and RPM >= 0.4 and Rate >= 300').copy()
        df2 = df.query('`Vehicle Type` in ["CAR", "SUV", "PICKUP"] and RPM >= 0.9 and Rate >= 450').copy()
        df_n = await nearest_coordinates(df1, truck, n_neighbors=18)
        df_n2 = await nearest_coordinates(df2, truck, n_neighbors=25)
        df_db3 = await find_veh_db3(df_n, df_n, df_n2, truck, filter_by=1.75)
        await bot.send_message(chat_id=chat_id, text=f"I was able to locate {df_db3.shape[0]} itineraries.")
        await card_sender_db3(chat_id, df_db3, input_text)
    elif option == "5":
        df1 = df.query('`Vehicle Type` in ["MOTORCYCLE", "ATV"] and RPM >= 0.3 and Rate >= 300').copy()
        df_n = await nearest_coordinates(df1, truck, n_neighbors=28)
        df_db3 = await find_veh_db4(df_n, truck, filter_by=1)
        await bot.send_message(chat_id=chat_id, text=f"I was able to locate {df_db3.shape[0]} itineraries.")
        await card_sender_db3(chat_id, df_db3, input_text)
    else:
        await bot.send_message(chat_id, "Option is not recognized, please click the option again.")
        return


async def card_sender(chat_id, df, truck):
    if df.empty:
        return

    df['Pick-Up Date'] = df['Pick-Up Date'].apply(lambda x: 'ASAP' if pd.to_datetime(x) <= pd.Timestamp(datetime.datetime.now()).normalize() else x)
    df['Pick-Up Date-2'] = df['Pick-Up Date-2'].apply(lambda x: 'ASAP' if pd.to_datetime(x) <= pd.Timestamp(datetime.datetime.now()).normalize() else x)
    df = df.sort_values('total_rpm', ascending=False).reset_index(drop=True)

    counter = 0
    
    for i in range(len(df)):
        p1 = ','.join([df.loc[i, 'Pickup City'], df.loc[i, 'Pickup State']])
        p2 = ','.join([df.loc[i, 'Pickup City-2'], df.loc[i, 'Pickup State-2']])
        d1 = ','.join([df.loc[i, 'Delivery City'], df.loc[i, 'Delivery State']])
        d2 = ','.join([df.loc[i, 'Delivery City-2'], df.loc[i, 'Delivery State-2']])
        # Routes
        route1 = '  =>  '.join([truck, p1, p2, d1, d2])
        route2 = '  =>  '.join([truck, p1, p2, d2, d1])
        route3 = '  =>  '.join([truck, p2, p1, d1, d2])
        route4 = '  =>  '.join([truck, p2, p1, d2, d1])

        origin = truck.replace(", ", "+").replace(",", "+").replace(" ", "+")
        pickup = '+'.join([df.loc[i, 'Pickup City'], df.loc[i, 'Pickup State'], df.loc[i, 'Pickup ZIP']]).replace(" ", "+")
        pickup2 = '+'.join([df.loc[i, 'Pickup City-2'], df.loc[i, 'Pickup State-2'], df.loc[i, 'Pickup ZIP-2']]).replace(" ", "+")
        delivery = '+'.join([df.loc[i, 'Delivery City'], df.loc[i, 'Delivery State'], df.loc[i, 'Delivery ZIP']]).replace(" ", "+")
        delivery2 = '+'.join([df.loc[i, 'Delivery City-2'], df.loc[i, 'Delivery State-2'], df.loc[i, 'Delivery ZIP-2']]).replace(" ", "+")

        min_route = df['min_route'].iloc[i]
        

        # Links
        link1 = f"https://www.google.com/maps/dir/?api=1&origin={origin}&waypoints={pickup}|{pickup2}|{delivery}&destination={delivery2}&travelmode=driving"
        link2 = f"https://www.google.com/maps/dir/?api=1&origin={origin}&waypoints={pickup}|{pickup2}|{delivery2}&destination={delivery}&travelmode=driving"
        link3 = f"https://www.google.com/maps/dir/?api=1&origin={origin}&waypoints={pickup2}|{pickup}|{delivery}&destination={delivery2}&travelmode=driving"
        link4 = f"https://www.google.com/maps/dir/?api=1&origin={origin}&waypoints={pickup2}|{pickup}|{delivery2}&destination={delivery}&travelmode=driving"

        card =  f"{hbold('Total Rate:')} 💵 ${round(df['total_rate'].iloc[i])}  🕘 ${round(df['total_rpm'].iloc[i], 2)}/mi  🚚 {round(df['total_distance'].iloc[i])}* miles\n" \
                f"{hbold('Vehicles:')} {(df['Vehicle Type'].iloc[i])}, {(df['Vehicle Type-2'].iloc[i])}\n" \
                f"{hbold('Route:')} {hlink(route1 if min_route == 0 else route2 if min_route == 1 else route3 if min_route == 2 else route4, link1 if min_route == 0 else link2 if min_route == 1 else link3 if min_route == 2 else link4)}\n" \
                f"{hbold('----------------------------------')}\n" \
                f"{hbold('Vehicle-1:')} {(df['Vehicle Info'].iloc[i])}\n" \
                f"{hbold('Price:')} ${df['Rate'].iloc[i]}, ${df['RPM'].iloc[i]}/mi ({df['Distance'].iloc[i]} miles)\n" \
                f"{hbold('Pickup Date:')} {(df['Pick-Up Date'].iloc[i])}\n" \
                f"{hbold('Pickup:')} {(p1)} ({df['Pickup metro area'].iloc[i]})\n" \
                f"{hbold('Delivery:')} {(d1)} ({df['Delivery metro area'].iloc[i]})\n" \
                f"{hbold('Order ID:')} {df['Order ID'].iloc[i]}\n" \
                f"{hbold('Company Name:')} {df['Company Name'].iloc[i]}\n" \
                f"{hbold('Contacts:')} {hcode(df['Phone'].iloc[i])}\n" \
                f"{hbold('Additional Info:')} {df['Additional Info'].iloc[i]}\n" \
                f"{hitalic(df['Vehicle Condition'].iloc[i])}, {hitalic(df['Trailer Type'].iloc[i])}\n" \
                f"{hbold('----------------------------------')}\n" \
                f"{hbold('Vehicle-2:')} {(df['Vehicle Info-2'].iloc[i])}\n" \
                f"{hbold('Price:')} ${df['Rate-2'].iloc[i]}, ${df['RPM-2'].iloc[i]}/mi ({df['Distance-2'].iloc[i]} miles)\n" \
                f"{hbold('Pickup Date:')} {(df['Pick-Up Date-2'].iloc[i])}\n" \
                f"{hbold('Pickup:')} {(p2)} ({df['Pickup metro area-2'].iloc[i]})\n" \
                f"{hbold('Delivery:')} {(d2)} ({df['Delivery metro area-2'].iloc[i]})\n" \
                f"{hbold('Order ID:')} {(df['Order ID-2'].iloc[i])}\n" \
                f"{hbold('Company Name:')} {(df['Company Name-2'].iloc[i])}\n" \
                f"{hbold('Contacts:')} {hcode(df['Phone-2'].iloc[i])}\n" \
                f"{hbold('Additional Info:')} {df['Additional Info-2'].iloc[i]}\n" \
                f"{hitalic(df['Vehicle Condition-2'].iloc[i])}, {hitalic(df['Trailer Type-2'].iloc[i])}\n" \
                

        await bot.send_message(chat_id=chat_id, text=card)
        counter += 1
        if counter % 10 == 0:
            await asyncio.sleep(2)


async def card_sender_db3(chat_id, df, truck):
    if df.empty:
        return

    df['Pick-Up Date'] = df['Pick-Up Date'].apply(lambda x: 'ASAP' if pd.to_datetime(x) <= pd.Timestamp(datetime.datetime.now()).normalize() else x)
    df['Pick-Up Date-2'] = df['Pick-Up Date-2'].apply(lambda x: 'ASAP' if pd.to_datetime(x) <= pd.Timestamp(datetime.datetime.now()).normalize() else x)
    df['Pick-Up Date-3'] = df['Pick-Up Date-3'].apply(lambda x: 'ASAP' if pd.to_datetime(x) <= pd.Timestamp(datetime.datetime.now()).normalize() else x)
    df = df.sort_values('total_rpm', ascending=False).reset_index(drop=True)

    counter = 0
    
    for i in range(len(df)):
        pickup1 = ','.join([df.loc[i, 'Pickup City'], df.loc[i, 'Pickup State'], df.loc[i, 'Pickup ZIP']])
        pickup2 = ','.join([df.loc[i, 'Pickup City-2'], df.loc[i, 'Pickup State-2'], df.loc[i, 'Pickup ZIP-2']])
        pickup3 = ','.join([df.loc[i, 'Pickup City-3'], df.loc[i, 'Pickup State-3'], df.loc[i, 'Pickup ZIP-3']])
        delivery1 = ','.join([df.loc[i, 'Delivery City'], df.loc[i, 'Delivery State'], df.loc[i, 'Delivery ZIP']])
        delivery2 = ','.join([df.loc[i, 'Delivery City-2'], df.loc[i, 'Delivery State-2'], df.loc[i, 'Delivery ZIP-2']])
        delivery3 = ','.join([df.loc[i, 'Delivery City-3'], df.loc[i, 'Delivery State-3'], df.loc[i, 'Delivery ZIP-3']])

        routes = []
        min_route = df['min_route'].iloc[i]
        for pick_up_permutation in itertools.permutations([pickup1, pickup2, pickup3]):
            for delivery_permutation in itertools.permutations([delivery1, delivery2, delivery3]):
                route = list(pick_up_permutation) + list(delivery_permutation)
                route_str = '|'.join(route)
                routes.append(route_str)
        selected_route = routes[min_route]
        if selected_route:
            waypoints, destination = '|'.join(selected_route.split('|')[:-1]), selected_route.split('|')[-1]
            route_states = [','.join(r.split(',')[:2]).strip() for r in selected_route.split('|')]
            truck_state = ','.join(truck.split(',')[:2]).strip()
            states = [truck_state] + route_states
            route_view = '  =>  '.join(states)

            link = "https://www.google.com/maps/dir/?api=1&origin={}&waypoints={}&destination={}&travelmode=driving".format(
                truck.replace(" ", "+"), waypoints.replace(" ", "+"), destination.replace(" ", "+")
            )

        card =  f"{hbold('Total Rate:')} 💵 ${round(df['total_rate'].iloc[i])}  🕘 ${round(df['total_rpm'].iloc[i], 2)}/mi  🚚 {round(df['total_distance'].iloc[i])}* miles\n" \
                f"{hbold('Vehicles:')} {(df['Vehicle Type'].iloc[i])}, {(df['Vehicle Type-2'].iloc[i])}, {(df['Vehicle Type-3'].iloc[i])}\n" \
                f"{hbold('Route:')} {hlink(route_view, link)}\n" \
                f"{hbold('----------------------------------')}\n" \
                f"{hbold('Vehicle-1:')} {(df['Vehicle Info'].iloc[i])}\n" \
                f"{hbold('Price:')} ${df['Rate'].iloc[i]}, ${df['RPM'].iloc[i]}/mi ({df['Distance'].iloc[i]} miles)\n" \
                f"{hbold('Pickup Date:')} {(df['Pick-Up Date'].iloc[i])}\n" \
                f"{hbold('Pickup:')} {(pickup1)} ({df['Pickup metro area'].iloc[i]})\n" \
                f"{hbold('Delivery:')} {(delivery1)} ({df['Delivery metro area'].iloc[i]})\n" \
                f"{hbold('Order ID:')} {df['Order ID'].iloc[i]}\n" \
                f"{hbold('Company Name:')} {df['Company Name'].iloc[i]}\n" \
                f"{hbold('Contacts:')} {hcode(df['Phone'].iloc[i])}\n" \
                f"{hbold('Additional Info:')} {df['Additional Info'].iloc[i]}\n" \
                f"{hitalic(df['Vehicle Condition'].iloc[i])}, {hitalic(df['Trailer Type'].iloc[i])}\n" \
                f"{hbold('----------------------------------')}\n" \
                f"{hbold('Vehicle-2:')} {(df['Vehicle Info-2'].iloc[i])}\n" \
                f"{hbold('Price:')} ${df['Rate-2'].iloc[i]}, ${df['RPM-2'].iloc[i]}/mi ({df['Distance-2'].iloc[i]} miles)\n" \
                f"{hbold('Pickup Date:')} {(df['Pick-Up Date-2'].iloc[i])}\n" \
                f"{hbold('Pickup:')} {(pickup2)} ({df['Pickup metro area-2'].iloc[i]})\n" \
                f"{hbold('Delivery:')} {(delivery2)} ({df['Delivery metro area-2'].iloc[i]})\n" \
                f"{hbold('Order ID:')} {(df['Order ID-2'].iloc[i])}\n" \
                f"{hbold('Company Name:')} {(df['Company Name-2'].iloc[i])}\n" \
                f"{hbold('Contacts:')} {hcode(df['Phone-2'].iloc[i])}\n" \
                f"{hbold('Additional Info:')} {df['Additional Info-2'].iloc[i]}\n" \
                f"{hitalic(df['Vehicle Condition-2'].iloc[i])}, {hitalic(df['Trailer Type-2'].iloc[i])}\n" \
                f"{hbold('----------------------------------')}\n" \
                f"{hbold('Vehicle-3:')} {(df['Vehicle Info-3'].iloc[i])}\n" \
                f"{hbold('Price:')} ${df['Rate-3'].iloc[i]}, ${df['RPM-3'].iloc[i]}/mi ({df['Distance-3'].iloc[i]} miles)\n" \
                f"{hbold('Pickup Date:')} {(df['Pick-Up Date-3'].iloc[i])}\n" \
                f"{hbold('Pickup:')} {(pickup3)} ({df['Pickup metro area-3'].iloc[i]})\n" \
                f"{hbold('Delivery:')} {(delivery3)} ({df['Delivery metro area-3'].iloc[i]})\n" \
                f"{hbold('Order ID:')} {(df['Order ID-3'].iloc[i])}\n" \
                f"{hbold('Company Name:')} {(df['Company Name-3'].iloc[i])}\n" \
                f"{hbold('Contacts:')} {hcode(df['Phone-3'].iloc[i])}\n" \
                f"{hbold('Additional Info:')} {df['Additional Info-3'].iloc[i]}\n" \
                f"{hitalic(df['Vehicle Condition-3'].iloc[i])}, {hitalic(df['Trailer Type-3'].iloc[i])}\n" \
                

        await bot.send_message(chat_id=chat_id, text=card)
        counter += 1
        if counter % 10 == 0:
            await asyncio.sleep(2)



async def city_state_to_coordinates(city, state, zip_code=None):
    if zip_code:
        # Build the API request URL with zip code
        url = f"https://nominatim.openstreetmap.org/search?q={city},{state},{zip_code},United States&format=json"
    else:
        # Build the API request URL without zip code
        url = f"https://nominatim.openstreetmap.org/search?q={city},{state},United States&format=json"
    
    # Make the API request
    async with aiohttp.ClientSession() as session:
        response = await session.get(url)
    
        # Extract the latitude and longitude from the response
        data = await response.json()
        if len(data) == 0:
            return None, None, None
        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])
        full_name = ', '.join([data[0]["display_name"].split(', ')[0], data[0]["display_name"].split(', ')[-2]])
    
    return lat, lon, full_name

async def nearest_coordinates(df, truck=[35.017089,-85.1323228], n_neighbors=20):
    X = np.deg2rad(df[['P_lat', 'P_lon']].values.astype(float))
    tree = BallTree(X, metric='haversine')
    distances, indices = tree.query(np.deg2rad([truck]), return_distance=True, k=n_neighbors)
    return df.iloc[indices[0]]


async def find_veh_db1(df, truck, filter_by=1):
    # Get all combinations of cargo pairs
    cargo_pairs = list(itertools.combinations(df.index, 2))

    # Calculate distances between pickups and deliveries for each pair
    distance_matrix_cars = []

    for i, j in cargo_pairs:
        pickup1 = (df.loc[i, 'P_lat'], df.loc[i, 'P_lon'])
        pickup2 = (df.loc[j, 'P_lat'], df.loc[j, 'P_lon'])
        delivery1 = (df.loc[i, 'D_lat'], df.loc[i, 'D_lon'])
        delivery2 = (df.loc[j, 'D_lat'], df.loc[j, 'D_lon'])
        
        route1 = [truck, pickup1, pickup2, delivery1, delivery2]
        route2 = [truck, pickup1, pickup2, delivery2, delivery1]
        route3 = [truck, pickup2, pickup1, delivery1, delivery2]
        route4 = [truck, pickup2, pickup1, delivery2, delivery1]

        routes = [route1, route2, route3, route4]
        distances = [sum(geodesic(route[k-1], route[k]).miles for k in range(1, len(route))) for route in routes]

        min_distance = min(distances)
        min_route = distances.index(min_distance)
        if min_distance < 925:
            road_multiplier = 1.23
        elif min_distance < 1100:
            road_multiplier = 1.21
        elif min_distance < 1300:
            road_multiplier = 1.19
        else:
            road_multiplier = 1.17
        total_distance = min_distance * road_multiplier
        total_rate = df.loc[i, 'Rate'] + df.loc[j, 'Rate']
        rpm = total_rate/total_distance
        distance_matrix_cars.append({"total_rate": total_rate, "total_distance": total_distance, "total_rpm": rpm, "min_route": min_route, **{col: df.loc[i, col] for col in df.columns}, **{col+'-2': df.loc[j, col] for col in df.columns}})
    

    notifications = [i for i in distance_matrix_cars if i['total_rpm'] >= filter_by]
    # Convert the distance matrix to a dataframe
    return pd.DataFrame(notifications)


async def find_veh_db2(df, df2, truck, filter_by=1.45):
    # FROM 2 Dataframes
    # Get all combinations of cargo pairs
    cargo_pairs = list(itertools.product(df.index, df2.index))

    # Calculate distances between pickups and deliveries for each pair
    distance_matrix_moto_car = []

    for i, j in cargo_pairs:
        pickup1 = (df.loc[i, 'P_lat'], df.loc[i, 'P_lon'])
        pickup2 = (df2.loc[j, 'P_lat'], df2.loc[j, 'P_lon'])
        delivery1 = (df.loc[i, 'D_lat'], df.loc[i, 'D_lon'])
        delivery2 = (df2.loc[j, 'D_lat'], df2.loc[j, 'D_lon'])
        
        route1 = [truck, pickup1, pickup2, delivery1, delivery2]
        route2 = [truck, pickup1, pickup2, delivery2, delivery1]
        route3 = [truck, pickup2, pickup1, delivery1, delivery2]
        route4 = [truck, pickup2, pickup1, delivery2, delivery1]

        routes = [route1, route2, route3, route4]
        distances = [sum(geodesic(route[k-1], route[k]).miles for k in range(1, len(route))) for route in routes]

        min_distance = min(distances)
        min_route = distances.index(min_distance)
        if min_distance < 925:
            road_multiplier = 1.23
        elif min_distance < 1100:
            road_multiplier = 1.21
        elif min_distance < 1300:
            road_multiplier = 1.19
        else:
            road_multiplier = 1.17
        total_distance = min_distance * road_multiplier
        total_rate = df.loc[i, 'Rate'] + df2.loc[j, 'Rate']
        rpm = total_rate/total_distance
        distance_matrix_moto_car.append({"total_rate": total_rate, "total_distance": total_distance, "total_rpm": rpm, "min_route": min_route, **{col: df.loc[i, col] for col in df.columns}, **{col+'-2': df2.loc[j, col] for col in df2.columns}})

    notifications = [i for i in distance_matrix_moto_car if i['total_rpm'] >= filter_by]
    # Convert the distance matrix to a dataframe
    return pd.DataFrame(notifications)


async def find_veh_db3(df, df2, df3, truck, filter_by=1.65):
    # FROM 3 Dataframes
    # Get all combinations of cargo pairs
    cargo_triplets = list(itertools.product(df.index, df2.index, df3.index))

    # Calculate distances between pickups and deliveries for each triple
    distance_matrix_moto_car = []

    for i, j, k in cargo_triplets:
        if i < j:
            pickup1 = (df.loc[i, 'P_lat'], df.loc[i, 'P_lon'])
            pickup2 = (df2.loc[j, 'P_lat'], df2.loc[j, 'P_lon'])
            pickup3 = (df3.loc[k, 'P_lat'], df3.loc[k, 'P_lon'])
            delivery1 = (df.loc[i, 'D_lat'], df.loc[i, 'D_lon'])
            delivery2 = (df2.loc[j, 'D_lat'], df2.loc[j, 'D_lon'])
            delivery3 = (df3.loc[k, 'D_lat'], df3.loc[k, 'D_lon'])

            G = nx.Graph()
            nodes = [pickup1, pickup2, pickup3, delivery1, delivery2, delivery3, truck]
            for node in nodes:
                G.add_node(node)
            for x in range(len(nodes) - 1):
                for y in range(x+1, len(nodes)):
                    G.add_edge(nodes[x], nodes[y], weight=geodesic(nodes[x], nodes[y]).miles)

            routes = []
            for pick_up_permutation in itertools.permutations([pickup1, pickup2, pickup3]):
                for delivery_permutation in itertools.permutations([delivery1, delivery2, delivery3]):
                    route = [truck] + list(pick_up_permutation) + list(delivery_permutation)
                    distances = []
                    for z in range(len(route) - 1):
                        distances.append(nx.shortest_path_length(G, route[z], route[z+1], weight='weight'))
                    sum_distance = sum(distances)
                    routes.append(sum_distance)

            min_distance = min(routes)
            min_route = routes.index(min_distance)
            road_multiplier = 1.25
            total_distance = round(min_distance * road_multiplier)
            total_rate = df.loc[i, 'Rate'] + df2.loc[j, 'Rate'] + df3.loc[k, 'Rate']
            rpm = round(total_rate / total_distance, 2)
            distance_matrix_moto_car.append({"total_rate": total_rate, "total_distance": total_distance, "total_rpm": rpm, "min_route": min_route, **{col: df.loc[i, col] for col in df.columns}, **{col+'-2': df2.loc[j, col] for col in df2.columns}, **{col+'-3': df3.loc[k, col] for col in df3.columns}})

    notifications = [i for i in distance_matrix_moto_car if i['total_rpm'] >= filter_by]
    return pd.DataFrame(notifications)


async def find_veh_db4(df, truck, filter_by=1.2):
    # FROM 3 Dataframes
    # Get all combinations of cargo pairs
    cargo_triplets = list(itertools.combinations(df.index, 3))

    # Calculate distances between pickups and deliveries for each triple
    distance_matrix_moto_car = []

    for i, j, k in cargo_triplets:
        pickup1 = (df.loc[i, 'P_lat'], df.loc[i, 'P_lon'])
        pickup2 = (df.loc[j, 'P_lat'], df.loc[j, 'P_lon'])
        pickup3 = (df.loc[k, 'P_lat'], df.loc[k, 'P_lon'])
        delivery1 = (df.loc[i, 'D_lat'], df.loc[i, 'D_lon'])
        delivery2 = (df.loc[j, 'D_lat'], df.loc[j, 'D_lon'])
        delivery3 = (df.loc[k, 'D_lat'], df.loc[k, 'D_lon'])

        G = nx.Graph()
        nodes = [pickup1, pickup2, pickup3, delivery1, delivery2, delivery3, truck]
        for node in nodes:
            G.add_node(node)
        for x in range(len(nodes) - 1):
            for y in range(x+1, len(nodes)):
                G.add_edge(nodes[x], nodes[y], weight=geodesic(nodes[x], nodes[y]).miles)

        routes = []
        for pick_up_permutation in itertools.permutations([pickup1, pickup2, pickup3]):
            for delivery_permutation in itertools.permutations([delivery1, delivery2, delivery3]):
                route = [truck] + list(pick_up_permutation) + list(delivery_permutation)
                distances = []
                for z in range(len(route) - 1):
                    distances.append(nx.shortest_path_length(G, route[z], route[z+1], weight='weight'))
                sum_distance = sum(distances)
                routes.append(sum_distance)

        min_distance = min(routes)
        min_route = routes.index(min_distance)
        road_multiplier = 1.25
        total_distance = round(min_distance * road_multiplier)
        total_rate = df.loc[i, 'Rate'] + df.loc[j, 'Rate'] + df.loc[k, 'Rate']
        rpm = round(total_rate / total_distance, 2)
        distance_matrix_moto_car.append({"total_rate": total_rate, "total_distance": total_distance, "total_rpm": rpm, "min_route": min_route, **{col: df.loc[i, col] for col in df.columns}, **{col+'-2': df.loc[j, col] for col in df.columns}, **{col+'-3': df.loc[k, col] for col in df.columns}})

    notifications = [i for i in distance_matrix_moto_car if i['total_rpm'] >= filter_by]
    return pd.DataFrame(notifications)




if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
