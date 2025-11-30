import time
import json
import random
import sys
import pandas as pd
from kafka import KafkaProducer

# ---------------------------------------------------------
# 1. Configuration
# ---------------------------------------------------------
KAFKA_HOST = 'localhost'  
KAFKA_PORT = 9092
TOPIC_NAME = 'bookings' # Kafka topic name

# The pause between sending each event to simulate a live stream
SLEEP_TIME_SECONDS = 0.5 

# ---------------------------------------------------------
# 2. Kafka Producer Setup
# ---------------------------------------------------------
print(f"Connecting to Kafka at {KAFKA_HOST}:{KAFKA_PORT}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[f'{KAFKA_HOST}:{KAFKA_PORT}'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        # Set a small batch size/linger to ensure immediate sending for streaming demo
        linger_ms=10, 
        batch_size=16384 
    )
    print("Producer Connected!")
except Exception as e:
    print(f"Connection Failed: {e}")
    sys.exit(1)

# ---------------------------------------------------------
# 3. Load CSV Data
# ---------------------------------------------------------
# Assuming CSV files (passenger.csv, flight.csv, ticket.csv, weather.csv) 
# are in the same directory as this script.
print("Loading CSV files...")
try:
    passengers = pd.read_csv('passenger.csv').to_dict(orient='records')
    flights = pd.read_csv('flight.csv').to_dict(orient='records')
    tickets = pd.read_csv('ticket.csv').to_dict(orient='records')
    weather_list = pd.read_csv('weather.csv').to_dict(orient='records')
    
    print(f"Data Loaded Successfully. Total tickets available: {len(tickets)}")

except FileNotFoundError as e:
    print(f"Critical Error: File not found ({e.filename})")
    sys.exit(1)

# ---------------------------------------------------------
# 4. Processing & Sending (Real-Time Stream Logic)
# ---------------------------------------------------------
print(f"\nStarting Real-Time Stream Upload to topic '{TOPIC_NAME}'...")
print(f"Streaming at a rate of 1 event every {SLEEP_TIME_SECONDS} seconds...")

i = 0
total_tickets = len(tickets)

# Use a while loop to simulate continuous streaming
while True:
    
    # Randomly select a record from each DataFrame to create an event
    # We cycle through the ticket list to ensure we use all ticket IDs eventually
    ticket_index = i % total_tickets 
    
    p = random.choice(passengers)
    f = random.choice(flights)
    w = random.choice(weather_list)

    event = {
        "event_time": time.strftime('%Y-%m-%d %H:%M:%S'),
        "ticket_id": tickets[ticket_index].get('ticket_id', str(random.randint(100000, 999999))),
        "flight_info": {
            "flight_id": f.get('flight_id'),
            "origin": f.get('origin_airport_code'),
            "dest": f.get('destination_airport_code'),
            "airline": f.get('carrier_name')
        },
        "passenger": {
            "id": p.get('passenger_id'),
            "name": f"{p.get('first_name')} {p.get('last_name')}",
            "gender": p.get('gender')
        },
        "price": tickets[ticket_index].get('price', random.randint(200, 1000)),
        "weather": w.get('weather_condition')
    }

    # Send the event immediately
    producer.send(TOPIC_NAME, value=event)
    producer.flush() 

    sys.stdout.write(f"\rEvent #{i}: Sent ticket ID {event['ticket_id']} for flight {event['flight_info']['flight_id']}...")
    sys.stdout.flush()

    i += 1
    time.sleep(SLEEP_TIME_SECONDS) # CRITICAL: Wait before sending the next event