import pandas as pd
from geopy.distance import geodesic
from itertools import permutations
from itertools import product
from sklearn.neighbors import BallTree
import numpy as np
import asyncio
import aiohttp

async def city_state_to_coordinates(city, state, zip_code=None):
    if zip_code:
        # Build the API request URL with zip code
        url = f"https://nominatim.openstreetmap.org/search?q={city},{state},{zip_code}&format=json"
    else:
        # Build the API request URL without zip code
        url = f"https://nominatim.openstreetmap.org/search?q={city},{state}&format=json"
    
    # Make the API request
    async with aiohttp.ClientSession() as session:
        response = await session.get(url)
    
        # Extract the latitude and longitude from the response
        data = await response.json()
        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])
        full_name = data[0]["display_name"]
    
    return lat, lon, full_name

async def nearest_coordinates(df, truck=[35.017089,-85.1323228], n_neighbors=20):
    X = np.deg2rad(df[['P_lat', 'P_lon']].values.astype(float))
    tree = BallTree(X, metric='haversine')
    distances, indices = tree.query(np.deg2rad([truck]), return_distance=True, k=n_neighbors)
    return df.iloc[indices[0]]


async def find_veh_db1(df, truck=[35.017089,-85.1323228], filter_by=1):
    # Get all combinations of cargo pairs
    cargo_pairs = list(permutations(df.index, 2))

    # Keep track of processed pairs to avoid duplicates
    processed_pairs = set()

    # Calculate distances between pickups and deliveries for each pair
    distance_matrix_cars = []

    for i, j in cargo_pairs:
        if (i, j) in processed_pairs or (j, i) in processed_pairs:
            continue
        processed_pairs.add((i, j))

        pickup1 = (df.loc[i, 'P_lat'], df.loc[i, 'P_lon'])
        pickup2 = (df.loc[j, 'P_lat'], df.loc[j, 'P_lon'])
        delivery1 = (df.loc[i, 'D_lat'], df.loc[i, 'D_lon'])
        delivery2 = (df.loc[j, 'D_lat'], df.loc[j, 'D_lon'])
        
        route1 = [truck, pickup1, pickup2, delivery1, delivery2]
        route2 = [truck, pickup1, pickup2, delivery2, delivery1]
        route3 = [truck, pickup2, pickup1, delivery1, delivery2]
        route4 = [truck, pickup2, pickup1, delivery2, delivery1]
        
        distances = [geodesic(route1[k-1], route1[k]).miles for k in range(1, len(route1))]
        distance1 = sum(distances)
        distances = [geodesic(route2[k-1], route2[k]).miles for k in range(1, len(route2))]
        distance2 = sum(distances)
        distances = [geodesic(route3[k-1], route3[k]).miles for k in range(1, len(route3))]
        distance3 = sum(distances)
        distances = [geodesic(route4[k-1], route4[k]).miles for k in range(1, len(route4))]
        distance4 = sum(distances)
        
        min_distance = min(distance1, distance2, distance3, distance4)
        min_route = 1 if distance1 == min_distance else 2 if distance2 == min_distance else 3 if distance3 == min_distance else 4
        road_multiplier = 1.2
        total_distance = min_distance * road_multiplier
        total_rate = df.loc[i, 'Rate'] + df.loc[j, 'Rate']
        rpm = total_rate/total_distance
        distance_matrix_cars.append({"total_rate": total_rate, "total_distance": total_distance, "total_rpm": rpm, "min_route": min_route, **{col: df.loc[i, col] for col in df.columns}, **{col+'-2': df.loc[j, col] for col in df.columns}})
    

    notifications = [i for i in distance_matrix_cars if i['total_rpm'] >= filter_by]
    # Convert the distance matrix to a dataframe
    return pd.DataFrame(notifications)


async def find_veh_db2(df, df2, truck=[35.017089,-85.1323228], filter_by=1.45):
    # FROM 2 Dataframes
    # Get all combinations of cargo pairs
    cargo_pairs = list(product(df.index, df2.index))

    # Keep track of processed pairs to avoid duplicates
    processed_pairs2 = set()

    # Calculate distances between pickups and deliveries for each pair
    distance_matrix_moto_car = []

    for i, j in cargo_pairs:
        if (i, j) in processed_pairs2 or (j, i) in processed_pairs2:
            continue
        processed_pairs2.add((i, j))

        pickup1 = (df.loc[i, 'P_lat'], df.loc[i, 'P_lon'])
        pickup2 = (df2.loc[j, 'P_lat'], df2.loc[j, 'P_lon'])
        delivery1 = (df.loc[i, 'D_lat'], df.loc[i, 'D_lon'])
        delivery2 = (df2.loc[j, 'D_lat'], df2.loc[j, 'D_lon'])
        
        route1 = [truck, pickup1, pickup2, delivery1, delivery2]
        route2 = [truck, pickup1, pickup2, delivery2, delivery1]
        route3 = [truck, pickup2, pickup1, delivery1, delivery2]
        route4 = [truck, pickup2, pickup1, delivery2, delivery1]
        
        distances = [geodesic(route1[k-1], route1[k]).miles for k in range(1, len(route1))]
        distance1 = sum(distances)
        distances = [geodesic(route2[k-1], route2[k]).miles for k in range(1, len(route2))]
        distance2 = sum(distances)
        distances = [geodesic(route3[k-1], route3[k]).miles for k in range(1, len(route3))]
        distance3 = sum(distances)
        distances = [geodesic(route4[k-1], route4[k]).miles for k in range(1, len(route4))]
        distance4 = sum(distances)
        
        min_distance = min(distance1, distance2, distance3, distance4)
        min_route = 1 if distance1 == min_distance else 2 if distance2 == min_distance else 3 if distance3 == min_distance else 4
        road_multiplier = 1.2
        total_distance = min_distance * road_multiplier
        total_rate = df.loc[i, 'Rate'] + df2.loc[j, 'Rate']
        rpm = total_rate/total_distance
        distance_matrix_moto_car.append({"total_rate": total_rate, "total_distance": total_distance, "total_rpm": rpm, "min_route": min_route, **{col: df.loc[i, col] for col in df.columns}, **{col+'-2': df.loc[j, col] for col in df.columns}})

    notifications = [i for i in distance_matrix_moto_car if i['total_rpm'] >= filter_by]
    # Convert the distance matrix to a dataframe
    return pd.DataFrame(notifications)


async def send_new_notifications(notifications, file_name):
    if not notifications.empty:
        previous_notifications = pd.read_csv(f'csv_data/{file_name}.csv')
        new_notifications = notifications[~notifications.isin(previous_notifications)].dropna()
        if not new_notifications.empty:
            previous_notifications = pd.concat([previous_notifications, new_notifications], ignore_index=True)
            previous_notifications.to_csv(f'csv_data/{file_name}.csv', index=False)
        return new_notifications
    else:
        return pd.DataFrame()


async def main():
    lat, lon, full_name = await city_state_to_coordinates('chattanooga', 'TN')
    truck = [lat, lon]
    df = pd.read_csv('csv_data/live.csv')
    df1 = df.query('`Vehicle Type` in ["CAR", "SUV", "PICKUP"]').copy()
    df_n = await nearest_coordinates(df1, truck, n_neighbors=20)
    df_db1 = await find_veh_db1(df_n, truck, filter_by=1.5)
    df_nn = await send_new_notifications(df_db1)
    df_nn.to_csv('test.csv', index=False)



if __name__ == '__main__':
    asyncio.run(main())