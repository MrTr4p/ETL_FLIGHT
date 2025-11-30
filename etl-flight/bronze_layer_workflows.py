import os
import kagglehub
import pandas as pd
from sqlalchemy import text
import common
import threading
import random
import faker
import constants
import api

fake = faker.Faker()



def download_kaggle_file():

    path = kagglehub.dataset_download("usdot/flight-delays")

    return path


def modify_airport(path):
    airport = pd.read_csv(f"{path}/airports.csv")
    column_mapping = {
        'IATA_CODE': 'iata_code',
        'AIRPORT': 'airport',
        'CITY': 'city',
        'STATE': 'state',
        'COUNTRY': 'country',
        'LATITUDE': 'latitude',
        'LONGITUDE': 'longitude'
    }
    
    airport = airport.rename(columns=column_mapping)
    return airport
    

def to_sql_airport(path, engine):
    airport = modify_airport(path)
    airport.to_csv(f'{os.getcwd()}/airport.csv')
    common.write_large_sql_chunks(f'{os.getcwd()}/airport.csv', engine, 'bronze_dim_airport', chunksize=1000)

def modify_flights(path):
    flights = pd.read_csv(f"{path}/flights.csv", low_memory=False,nrows=10000)
    
    column_mapping = {
        'YEAR': 'year',
        'MONTH': 'month', 
        'DAY': 'day',
        'DAY_OF_WEEK': 'day_of_week',
        'carrier_code': 'carrier_code',
        'FLIGHT_NUMBER': 'flight_number',
        'TAIL_NUMBER': 'tail_number',
        'ORIGIN_AIRPORT': 'origin_airport',
        'DESTINATION_AIRPORT': 'destination_airport',
        'SCHEDULED_DEPARTURE': 'scheduled_departure',
        'DEPARTURE_TIME': 'departure_time',
        'DEPARTURE_DELAY': 'departure_delay',
        'TAXI_OUT': 'taxi_out',
        'WHEELS_OFF': 'wheels_off',
        'SCHEDULED_TIME': 'scheduled_time',
        'ELAPSED_TIME': 'elapsed_time',
        'AIR_TIME': 'air_time',
        'DISTANCE': 'distance',
        'WHEELS_ON': 'wheels_on',
        'TAXI_IN': 'taxi_in',
        'SCHEDULED_ARRIVAL': 'scheduled_arrival',
        'ARRIVAL_TIME': 'arrival_time',
        'ARRIVAL_DELAY': 'arrival_delay',
        'DIVERTED': 'diverted',
        'CANCELLED': 'cancelled',
        'CANCELLATION_REASON': 'cancellation_reason',
        'AIR_SYSTEM_DELAY': 'air_system_delay',
        'SECURITY_DELAY': 'security_delay',
        'AIRLINE_DELAY': 'airline_delay',
        'LATE_AIRCRAFT_DELAY': 'late_aircraft_delay',
        'WEATHER_DELAY': 'weather_delay',
        'IATA_CODE': 'iata_code',
        'AIRLINE': 'airline'
    }
    
    flights = flights.rename(columns=column_mapping)
    return flights

def create_flight_csv(path):
    flights = modify_flights(path)
    flights.to_csv(f'{os.getcwd()}/flights_modified.csv', index=False)

def modify_airlines(path):
    airlines = pd.read_csv(f"{path}/airlines.csv")
    
    column_mapping = {
        'AIRLINE': 'airline',
        'IATA_CODE': 'iata_code',
    }
    
    airlines = airlines.rename(columns=column_mapping)
    return airlines

def create_airline_csv(path):
    airlines = modify_airlines(path)
    airlines.to_csv(f'{os.getcwd()}/airlines_modified.csv', index=False)

def add_columns_in_chunks(main_csv_path, additional_csv_path,left_on, right_on, output_path, chunksize=10000):
    additional_df = pd.read_csv(additional_csv_path)
    
    first_chunk = True
    for chunk in common.read_large_csv_chunks(main_csv_path, chunksize=chunksize):
        merged_chunk = pd.merge(chunk, additional_df, left_on=left_on,right_on=right_on, how='left')
        merged_chunk = merged_chunk.rename(columns={'airline_x': "carrier_code", "airline_y": "carrier_name"})
        print(merged_chunk)
        merged_chunk.drop('iata_code', axis=1)            
        merged_chunk['aircrafttype'] = random.choice(constants.aircraftTypes)
        mode = 'w' if first_chunk else 'a'
        header = first_chunk
        
        merged_chunk.to_csv(output_path, mode=mode, header=header, index=False)
        first_chunk = False

def merge_flight_airline_csv(path, engine):
    flight = threading.Thread(target=create_flight_csv, args=(path,))
    airline =threading.Thread(target=create_airline_csv, args=(path,))
    flight.start()
    print("FLIGHT:FLIGHT_SUB_THREAD_HAS_STARTED")
    airline.start()
    print("FLIGHT:AIRELINE_SUB_THREAD_HAS_STARTED")

    airline.join()
    print('FLIGHT:AIRLINE_SUB_THREAD_HAS_FINISHED')
    flight.join()
    print("FLIGHT:FLIGHT_SUB_THREAD_HAS_FINISHED")

    add_columns_in_chunks("./flights_modified.csv", "./airlines_modified.csv" ,"airline", "iata_code", "flights_airlines.csv", chunksize=10000)

    common.write_large_sql_chunks(f"{os.getcwd()}/flights_airlines.csv", engine, 'bronze_dim_flight', chunksize=10000)


def create_passenger(engine):
    with engine.connect() as connection:
        for i in range(1000):
            gender = random.choice(['MALE', 'FEMALE'])
            connection.execute(text(f"""
            INSERT INTO bronze_dim_passenger
            (id, firstname, gender, lastname, age, nationality, loyalitystatus, hascomplaints)
            VALUES 
            ({i}, '{fake.first_name_male() if gender == 'MALE' else fake.first_name_female()}', 
            '{gender}', '{fake.last_name()}', {random.randint(10,120)}, 
            '{random.choice(constants.nationalities)}', 
            '{random.choice(constants.loyalty_statuses)}', 
            {random.choice([True,False])})
             """))
            if i % 100 ==0:
                print(f"PASSENGER: Progress: {i}/1000")

        
            
def create_payment(engine):
    with engine.connect() as connection:
        for i in range(1000):
            connection.execute(text(f"""
            INSERT INTO bronze_dim_payment
            (id, paymentmethod, currency, paymentstatus)
            VALUES 
            ({i}, '{random.choice(constants.paymentMethods)}', '{random.choice(constants.currency_codes)}', '{random.choice(constants.paymentStatus)}')
        """))
            if i % 100 ==0:
                print(f"PAYMENT: Progress: {i}/1000")

        
            
def create_ticket(engine):
    with engine.connect() as connection:
        for i in range(1000):
            connection.execute(text(f"""
            INSERT INTO bronze_dim_ticket
            (id, bookingclass, seatnumber, faretype, baggageallowance)
            VALUES 
            ({i}, '{random.choice(constants.booking_classes)}', '{random.randint(1, 90)}', '{random.choice(constants.fare_types)}', '{random.randint(10, 10000)}')
        """))
        if i % 100 ==0:
                print(f"TICKET: Progress: {i}/1000")
        
            

def create_date(engine):
    with engine.connect() as connection:
        for i, data_record in enumerate(common.generate_calendar_dimension('2015-01-01', '2015-12-31')):
            connection.execute(text(f"""
            INSERT INTO bronze_dim_date 
            (id, calenderdate, dayofweek, dayofmonth, monthnumber, monthname, quarter, year, isweekend)
            VALUES 
            ({i+1}, '{int(data_record['calenderdate'].replace('-', ''))}', {data_record['dayofweek']}, 
             {data_record['dayofmonth']}, {data_record['monthnumber']}, '{data_record['monthname']}', 
             {data_record['quarter']}, {data_record['year']}, {data_record['isweekend']})
            """))
        

def create_weather(engine):
    with engine.connect() as connection:
        for i in range(1,80):
            random_airport_code = random.choice(constants.usa_iata_codes)
            airport = connection.execute(
            text(f"SELECT * FROM bronze_dim_airport WHERE iata_code = '{random_airport_code}'")
            ).fetchone()
            date = connection.execute(text(f"SELECT * FROM bronze_dim_date WHERE id = {i}")).fetchone()
            lat = airport[5]
            long = airport[6]
            calender_date =date[1]

            weather_data = api.get_historical_weather(calender_date, lat,long)
            print(i)
            if weather_data == None:
                print(f'Error at {i}')
                continue
            connection.execute(text(f"""
            INSERT INTO bronze_dim_weather 
            (id, airportiata, dateid, temperaturec, precipitationmm, windspeedkph, weathercondition)
            VALUES 
            ({i}, '{random_airport_code}', {i}, {weather_data['temperatureC']}, {weather_data['precipitationmm']}, {weather_data['windspeedkph']}, '{weather_data['weathercondition']}')
        """))
            

            
        
def create_booking(engine):
    with engine.connect() as connection:
        for i in range(1, 10000):
            connection.execute(text(f"""
                INSERT INTO bronze_fact_booking
                (id, passengerid, flightid, ticketid, paymentid, bookingdateid, departuredateid, basefareamount, netsalesamount, numberofbags, totalweightkg)
                VALUES 
                ({i}, {random.randint(1, 10000)}, {random.randint(1, 5000000)}, {random.randint(1, 10000)}, 
                {random.randint(1, 10000)}, {random.randint(1, 365)}, {random.randint(1, 365)}, {random.randint(1, 2000)},
                {random.randint(1, 3000)}, {random.randint(1, 10)}, {random.randint(10, 1000)})
            """))


            if i % 10000 ==0:
                print(f"BOOKING: Progress: {i}/500,000")
        
        