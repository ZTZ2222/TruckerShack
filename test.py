import pandas as pd
from geopy.distance import geodesic
import aiohttp
import itertools
import networkx as nx
import asyncio
from dispatch_bot import nearest_coordinates

class Node:
    def __init__(self, name, city, state, zip_code, coordinates):
        self.name = name
        self.city = city
        self.state = state
        self.zip_code = zip_code
        self.coordinates = coordinates

def find_min_route(G, truck, pickup1, pickup2, pickup3, delivery1, delivery2, delivery3):
    min_route_as_names = []
    min_route_as_coordinates = []
    min_route_for_link = []
    min_distance = float('inf')

    for pick_up_permutation in itertools.permutations([pickup1, pickup2, pickup3]):
        for delivery_permutation in itertools.permutations([delivery1, delivery2, delivery3]):
            route = [truck] + list(pick_up_permutation) + list(delivery_permutation)
            distances = []
            for z in range(len(route) - 1):
                distances.append(nx.shortest_path_length(G, route[z], route[z+1], weight='weight'))
            route_distance = sum(distances)
            if route_distance < min_distance:
                min_route_as_names = [f"{node.city}, {node.state}" for node in route]
                min_route_for_link = [f"{node.city},{node.state},{node.zip_code}" for node in route]
                min_route_as_coordinates = [node.coordinates for node in route]
                min_distance = route_distance
                
    
    return min_route_as_names, min_route_as_coordinates, round(min_distance*1.17), min_route_for_link


async def find_veh_db3(df, df2, df3, truck, filter_by=1.65):
    # FROM 3 Dataframes
    # Get all combinations of cargo pairs
    cargo_triplets = list(itertools.product(df.index, df2.index, df3.index))

    # Keep track of processed triples to avoid duplicates
    processed_triplets = set()

    # Calculate distances between pickups and deliveries for each triple
    distance_matrix_moto_car = []

    for i, j, k in cargo_triplets:
        if i < j:
            if (i, j, k) in processed_triplets or (i, k, j) in processed_triplets or (j, i, k) in processed_triplets or (j, k, i) in processed_triplets or (k, i, j) in processed_triplets or (k, j, i) in processed_triplets:
                continue
            processed_triplets.add((i, j, k))

            pickup1 = Node("pickup1", df.loc[i, 'Pickup City'], df.loc[i, 'Pickup State'], df.loc[i, 'Pickup ZIP'], (df.loc[i, 'P_lat'], df.loc[i, 'P_lon']))
            pickup2 = Node("pickup2", df2.loc[j, 'Pickup City'], df2.loc[j, 'Pickup State'], df2.loc[j, 'Pickup ZIP'], (df2.loc[j, 'P_lat'], df2.loc[j, 'P_lon']))
            pickup3 = Node("pickup3", df3.loc[k, 'Pickup City'], df3.loc[k, 'Pickup State'], df3.loc[k, 'Pickup ZIP'], (df3.loc[k, 'P_lat'], df3.loc[k, 'P_lon']))
            delivery1 = Node("delivery1", df.loc[i, 'Delivery City'], df.loc[i, 'Delivery State'], df.loc[i, 'Delivery ZIP'], (df.loc[i, 'D_lat'], df.loc[i, 'D_lon']))
            delivery2 = Node("delivery2", df2.loc[j, 'Delivery City'], df2.loc[j, 'Delivery State'], df2.loc[j, 'Delivery ZIP'], (df2.loc[j, 'D_lat'], df2.loc[j, 'D_lon']))
            delivery3 = Node("delivery3", df3.loc[k, 'Delivery City'], df3.loc[k, 'Delivery State'], df3.loc[k, 'Delivery ZIP'], (df3.loc[k, 'D_lat'], df3.loc[k, 'D_lon']))

            G = nx.Graph()

            nodes = [pickup1, pickup2, pickup3, delivery1, delivery2, delivery3, truck]

            for node in nodes:
                G.add_node(node)

            for x in range(len(nodes) - 1):
                for y in range(x+1, len(nodes)):
                    G.add_edge(nodes[x], nodes[y], weight=geodesic(nodes[x].coordinates, nodes[y].coordinates).miles)

            min_route_as_names, min_route_as_coordinates, min_distance, min_route_for_link = find_min_route(G, truck, pickup1, pickup2, pickup3, delivery1, delivery2, delivery3)

            
            total_rate = df.loc[i, 'Rate'] + df2.loc[j, 'Rate'] + df3.loc[k, 'Rate']
            rpm = round(total_rate / min_distance, 2)
            distance_matrix_moto_car.append({"total_rate": total_rate, "total_distance": min_distance, "total_rpm": rpm, "min_route_as_names": min_route_as_names, "min_route_as_coordinates": min_route_as_coordinates, "min_route_for_link": min_route_for_link, **{col: df.loc[i, col] for col in df.columns}, **{col+'-2': df2.loc[j, col] for col in df2.columns}, **{col+'-3': df3.loc[k, col] for col in df3.columns}})

    notifications = [i for i in distance_matrix_moto_car if i['total_rpm'] >= filter_by]
    return pd.DataFrame(notifications)


async def get_route_distance(df, filter_by=1.65):
    for i in range(len(df)):
        route = df.loc[i, 'min_route_as_coordinates']
        transformed_route = [f"{coord[1]},{coord[0]}" for coord in route]
        url = f"http://router.project-osrm.org/route/v1/driving/{';'.join(transformed_route)}?overview=false&steps=false"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                data = await resp.json()
                distance_meters = data['routes'][0]['distance']
                df.loc[i, 'total_distance'] = round(distance_meters / 1609.34)
                df.loc[i, 'total_rpm'] = round(df.loc[i, 'total_rate']/df.loc[i, 'total_distance'], 2)
                return df.query('total_rpm > @filter_by').copy()


async def main():
    canada = ['AB','BC','MB','NB','NL','NS','ON','PE','QC','SK','Ca']
    df_l = pd.read_csv('csv_data/live.csv').drop_duplicates(subset=['Pickup City', 'Vehicle Info', 'Delivery City']).drop_duplicates(subset=['Pickup City', 'Vehicle Info', 'Company Name']).drop_duplicates(subset=['Company Name', 'Vehicle Info', 'Delivery City']).reset_index(drop=True)
    df = df_l.query('`Pickup State` not in @canada and `Delivery State` not in @canada').copy()
    df = df[df['Distance'] > 0]
    df['RPM'] = round(df['Rate']/df['Distance'], 2)
    df = df[~df['Vehicle Info'].str.contains(r'[4-9]\sunit|1[0-5]\sunit', regex=True)]
    df1 = df.query('`Vehicle Type` in ["MOTORCYCLE", "ATV"] and RPM >= 0.4 and Rate >= 300').copy()
    df2 = df.query('`Vehicle Type` in ["CAR", "SUV", "PICKUP"] and RPM >= 1 and Rate >= 450').copy()
    # truck = (34.052235, -118.243683)
    truck = Node("truck", "Cherryville", "NC", 28021, (35.3813808, -81.3835815))
    df_n = await nearest_coordinates(df1, truck.coordinates, n_neighbors=15)
    df_n2 = await nearest_coordinates(df2, truck.coordinates, n_neighbors=25)
    df_n_n = await find_veh_db3(df_n, df_n, df_n2, truck, filter_by=1.65)
    df_n_apr = await get_route_distance(df_n_n, 1.65)
    df_n_apr.to_csv('test.csv', index=False)



if __name__ == '__main__':
    asyncio.run(main())
