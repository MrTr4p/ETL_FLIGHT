import time
import json
import random
from kafka import KafkaProducer

KAFKA_BROKER = 'kafka:9092'  
KAFKA_TOPIC = 'flight_bookings'

CARRIERS = ["United", "Delta", "American", "Southwest", "Spirit"]
AIRPORTS = ["LAX", "JFK", "SFO", "ATL", "DFW", "ORD", "MIA", "SEA"]
FLIGHT_STATUSES = ["Booked", "Confirmed", "Pending", "Cancelled"]
GENDERS = ["M", "F"]
NATIONALITIES = ["USA", "CAN", "MEX", "GBR", "FRA"]

def generate_flight_booking(booking_id):
    """Generates a single fake flight booking event."""
    origin, dest = random.sample(AIRPORTS, 2)
    carrier = random.choice(CARRIERS)
    
    return {
        "booking_id": booking_id,
        "passenger_id": random.randint(10000, 99999),
        "first_name": random.choice(["Alex", "Sarah", "Mike", "Emily", "Chris"]),
        "last_name": random.choice(["Smith", "Jones", "Davis", "Miller", "Wilson"]),
        "gender": random.choice(GENDERS),
        "age": random.randint(18, 75),
        
        "flight_number": f"{carrier[:2].upper()}{random.randint(100, 999)}",
        "carrier_name": carrier,
        "origin_airport_code": origin,
        "destination_airport_code": dest,
        "scheduled_departure": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() + random.randint(3600, 86400))),
        "flight_status": random.choice(FLIGHT_STATUSES)
    }

print(f"Connecting to Kafka at {KAFKA_BROKER}...")

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    print("ACTION REQUIRED: Ensure Kafka is running and port 9092 is mapped correctly to localhost.")
    exit()

print(f"Starting production to topic: {KAFKA_TOPIC}. Press Ctrl+C to stop.")

i = 0
while True:
    i += 1
    booking_data = generate_flight_booking(i)
    
    producer.send(KAFKA_TOPIC, value=booking_data)
    
    if i % 10 == 0:
        print(f"Sent {i} bookings. Last booking: {booking_data['flight_number']} ({booking_data['origin_airport_code']} -> {booking_data['destination_airport_code']})")

    time.sleep(1)


# docker cp notebooks/producer.py spark-notebook:/tmp/kafka_streaming_job.py              

#docker exec -it spark-notebook pip install kafka-python


#    docker exec -it --user root spark-notebook /usr/local/spark/bin/spark-submit \
#      --master spark://spark-master:7077 \
#      --name NotebookStreamingConsumer \
#      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
#   /tmp/kafka_streaming_job.py