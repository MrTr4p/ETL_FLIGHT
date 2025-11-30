from sqlalchemy import create_engine
import bronze_layer_workflows
import threading
import common

engine = create_engine('postgresql://postgres:postgres@dwh-postgres:5432/dwh_database')

if __name__ == "__main__":

    common.truncate_all_tables(engine)
    path = bronze_layer_workflows.download_kaggle_file()
    airportThread = threading.Thread(target=bronze_layer_workflows.to_sql_airport, args=(path, engine,))
    flightThread = threading.Thread(target=bronze_layer_workflows.merge_flight_airline_csv, args=(path, engine,))
    dateThread = threading.Thread(target=bronze_layer_workflows.create_date, args=(engine,))
    passengerThread = threading.Thread(target=bronze_layer_workflows.create_passenger, args=(engine,))
    paymentThread = threading.Thread(target=bronze_layer_workflows.create_payment, args=(engine,))
    ticketThread = threading.Thread(target=bronze_layer_workflows.create_ticket, args=(engine,))
    weatherThread = threading.Thread(target=bronze_layer_workflows.create_weather, args=(engine,))
    bookingThread = threading.Thread(target=bronze_layer_workflows.create_booking, args=(engine,))
    print(path)

    airportThread.start()
    print('AIRPORT_THREAD_STARTED')
    flightThread.start()
    print('FLIGHT_THREAD_STARTED')
    flightThread.join()
    print('FLIGHT_THREAD_FINISHED')
  
    passengerThread.start()
    print('PASSENGER_THREAD_STARTED')
    paymentThread.start()
    print('PAYMENT_THREAD_STARTED')
    ticketThread.start()
    print('TICKET_THREAD_STARTED')
   
    bookingThread.start()
    print('BOOKING_THREAD_STARTED')

    airportThread.join()
    print('AIRPORT_THREAD_FINISHED')
    
    passengerThread.join()
    print('PASSENGER_THREAD_FINISHED')
    paymentThread.join()
    print('PAYMENT_THREAD_FINISHED')
    ticketThread.join()
    print('TICKET_THREAD_FINISHED')
    bookingThread.join()
    print('BOOKING_THREAD_FINISHED')
    dateThread.start()
    print('DATE_THREAD_STARTED')
    dateThread.join()
    print('DATE_THREAD_FINISHED')
    weatherThread.start()
    print('WEATHER_THREAD_STARTED')
    weatherThread.join()
    print('WEATHER_THREAD_FINISHED')