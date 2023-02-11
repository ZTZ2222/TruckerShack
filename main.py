from extract_token import collect_token
import pandas as pd
import asyncio
from datetime import datetime
import aiohttp


url = 'https://prod-search-app-bff.awscal2.manheim.com/api/open-search'

headers = {
    'authority': 'prod-search-app-bff.awscal2.manheim.com',
    'accept': 'application/json',
    'accept-language': 'en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7',
    'authorization': 'Bearer token',
    'content-type': 'application/json',
    'origin': 'https://app.centraldispatch.com',
    'referer': 'https://app.centraldispatch.com/',
    'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
}
json_data = {
    'vehicleCount': {'min': 1,'max': 1,},
    'postedWithinHours': None,
    'tagListingsPostedWithin': 2,
    'trailerTypes': [],
    'paymentTypes': [],
    'vehicleTypes': [],
    'operability': 'all',
    'minimumPaymentTotal': None,
    'readyToShipWithinDays': '14',
    'minimumPricePerMile': None,
    'offset': 0,
    'limit': 500,
    'sortFields': [{'name': 'PICKUP', 'direction': 'ASC',},{'name': 'DELIVERYMETROAREA','direction': 'ASC',},],
    'shipperIds': [],
    'desiredDeliveryDate': None,
    'displayBlockedShippers': False,
    'searchByActiveOffers': None,
    'showPreferredShippersOnly': False,
    'showTaggedOnTop': False,
    'requestType': 'Open',
    'locations': [],
}

async def get_data(session, headers, offset):
    global cur_time
    cur_time = datetime.now().strftime('%m_%d_%Y_%H_%M')
    json_data['offset'] = offset
    async with session.post(url=url, headers=headers, json=json_data) as response:
        data = await response.json()
        items = data.get('items')
        for item in items:
            #Pickup information
            pu_city = item.get('origin').get('city')
            pu_state = item.get('origin').get('state')
            pu_zip = item.get('origin').get('zip')
            # pu_info = f"{pu_state}: {pu_city}, {pu_zip}"
            pu_metro_area = item.get('origin').get('metroArea')
            pu_lon = item.get('origin').get('geoCode').get('longitude')
            pu_lat = item.get('origin').get('geoCode').get('latitude')
            #Delivery information
            del_city = item.get('destination').get('city')
            del_state = item.get('destination').get('state')
            del_zip = item.get('destination').get('zip')
            # del_info = f"{del_state}: {del_city}, {del_zip}"
            del_metro_area = item.get('destination').get('metroArea')
            del_lon = item.get('destination').get('geoCode').get('longitude')
            del_lat = item.get('destination').get('geoCode').get('latitude')
            #Price and distance information
            rate = int(item.get('price').get('total'))
            other_method = item.get('price').get('balance')
            if len(other_method) < 2:
                pay_method = "COD/COP"
            else:
                pay_method = item.get('price').get('balance').get('paymentTime')
            distance = int(item.get('distance'))
            #Vehicle information
            vehicles = item.get('vehicles')
            for vehicle in vehicles:
                vehicle_type = vehicle.get('vehicleType')
                vehicle_year = vehicle.get('year')
                vehicle_make = vehicle.get('make')
                vehicle_model = vehicle.get('model')
                vehicle_qty = int(vehicle.get('qty'))
                vehicle_info = f"{vehicle_year} {vehicle_make} {vehicle_model}"
            check_condition = item.get('hasInOpVehicle')
            if check_condition == True:
                vehicle_condition = 'Inop'
            else:
                vehicle_condition = 'Runs'
            trailer_type = item.get('trailerType')
            #Pickup date
            pu_date = item.get('availableDate').split('T')[0]
            #Order and shipper info
            order_id = item.get('shipperOrderId')
            add_info = item.get('additionalInfo')
            company_name = item.get('shipper').get('companyName')
            phone = item.get('shipper').get('phone')
            email = item.get('shipper').get('email')
            rating = item.get('shipper').get('rating')

            result_data.append({
                'Vehicle Type': vehicle_type,
                'Pickup City': pu_city,
                'Pickup State': pu_state,
                'Pickup ZIP': pu_zip,
                'Delivery City': del_city,
                'Delivery State': del_state,
                'Delivery ZIP': del_zip,
                'Rate': rate,
                'Payment Method': pay_method,
                'Distance': distance,
                'Veh_qty': vehicle_qty,
                'Vehicle Info': vehicle_info,
                'Vehicle Condition': vehicle_condition,
                'Trailer Type': trailer_type,
                'Pick-Up Date': pu_date,
                'Order ID': order_id,
                'Additional Info': add_info,
                'Company Name': company_name,
                'Phone': phone,
                'Email': email,
                'Rating': rating,
                'Pickup metro area': pu_metro_area,
                'Delivery metro area': del_metro_area,
                'P_lat': pu_lat,
                'P_lon': pu_lon,
                'D_lat': del_lat,
                'D_lon': del_lon,
                'Time': cur_time
            })

async def gather_data():
    global result_data
    result_data = []
    token_parser = open('token.txt', 'r').read()
    headers['authorization'] = token_parser
    #Cars
    json_data['trailerTypes'] = ['ENCLOSED']
    json_data['vehicleTypes'] = ['Car','SUV','Pickup']
    async with aiohttp.ClientSession() as session:
        response = await session.post(url=url, headers=headers, json=json_data)
        status_code = response.status
        if status_code == 200:
            data = await response.json()
            total_items = data.get('totalRecords')
        elif status_code == 401 or 403:
            token_parser = collect_token()
            headers['authorization'] = token_parser
            response = await session.post(url=url, headers=headers, json=json_data)
            data = await response.json()
            total_items = data.get('totalRecords')
        else:
            print(status_code)
        tasks = []
        for offset in range(0, total_items, 500):
            task = asyncio.create_task(get_data(session, headers, offset))
            tasks.append(task)  
        await asyncio.gather(*tasks)
        await asyncio.sleep(1.0)
        #Moto
        json_data['trailerTypes'] = []
        json_data['vehicleTypes'] = ['ATV','Motorcycle']
        response = await session.post(url=url, headers=headers, json=json_data)
        status_code = response.status
        if status_code == 200:
            data = await response.json()
            total_items = data.get('totalRecords')
        elif status_code == 401 or 403:
            token_parser = collect_token()
            headers['authorization'] = token_parser
            response = await session.post(url=url, headers=headers, json=json_data)
            data = await response.json()
            total_items = data.get('totalRecords')
        else:
            print(status_code)
        tasks = []
        for offset in range(0, total_items, 500):
            task = asyncio.create_task(get_data(session, headers, offset))
            tasks.append(task)  
        await asyncio.gather(*tasks)
    df = pd.DataFrame(result_data).fillna('Not specified').replace('', 'Not specified')
    df = df[~df['Vehicle Info'].str.contains("^[4-9]|1.\s+unit.$", regex=True)]
    df.to_csv(f'csv_data/csv_hourly/{cur_time}.csv', index=False) 
    df.to_csv('csv_data/live.csv', index=False)
    df.to_csv('/home/ztz/Desktop/MyProjects/website/csv_data/live.csv', index=False)
    


async def main():
    while True:
        await gather_data()
        await asyncio.sleep(180)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


