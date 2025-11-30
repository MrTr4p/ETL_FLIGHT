import pandas as pd
import csv
from sqlalchemy import text


def write_large_sql_chunks(csv_path,engine, table_name, chunksize=10000):
    with engine.connect() as connection:
        connection.execute(text(f"TRUNCATE TABLE {table_name}"))
    first = True
    for i, chunk in enumerate(read_large_csv_chunks(csv_path,chunksize)):
        chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace' if first else 'append',
            index=False,
            method='multi'
        )
        first= False
        print(f"Chunk {i+1} written to '{table_name}'")
    
    print(f"Completed writing {table_name}")


def read_large_csv_chunks(csv_path, chunksize=10000):
    for chunk in pd.read_csv(csv_path, chunksize=chunksize, low_memory=False, index_col=0):
        yield chunk

def write_large_csv_chunks(csv_in_path,csv_out_path, chunksize=10000):
    first = True
    
    for chunk in read_large_csv_chunks(csv_in_path, chunksize):
        if chunk.empty:
            print("No data to write")
            continue
            
        mode = 'w' if first else 'a'
        header = first 
        chunk.to_csv(csv_out_path, mode=mode, header=header, index=False)
        first = False

    

def generate_calendar_dimension(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    calendar_data = []
    for date in date_range:
        calendar_data.append({
            'calenderdate': date.strftime('%Y-%m-%d'),
            'dayofweek': date.dayofweek + 1,
            'dayofweek_name': date.strftime('%A'),
            'dayofmonth': date.day,
            'monthnumber': date.month,
            'monthname': date.strftime('%B'),
            'quarter': (date.month - 1) // 3 + 1,
            'year': date.year,
            'isweekend': True if date.weekday() >= 5 else False,  
        })
    
    return calendar_data


def truncate_all_tables(engine):
    table_names = [
        'bronze_dim_airport',
        'bronze_dim_flight', 
        'bronze_dim_passenger',
        'bronze_dim_payment',
        'bronze_dim_ticket',
        'bronze_dim_date',
        'bronze_dim_weather',
        'bronze_fact_booking'
    ]
    
    with engine.connect() as connection:
        for table_name in table_names:
                connection.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
           
        print("All tables truncated successfully")