import requests
import constants

def get_historical_weather(date, latitude, longitude):
    date_str = str(date)
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'start_date': f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
        'end_date': f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
        'daily': ['temperature_2m_max', 'precipitation_sum', 'windspeed_10m_max', 'weathercode'],
        'temperature_unit': 'celsius',
        'windspeed_unit': 'kmh',
        'precipitation_unit': 'mm',
        'timezone': 'auto'
    }
    response = requests.get(constants.meteo_url, params=params)
    if response.status_code == 200:
        if len(response.text) == 0:
            return None
        data = response.json()['daily']
        
        return {
            'response_text': response.text[:500],
            'date': date,
            'temperatureC': int(data['temperature_2m_max'][0]),
            'precipitationmm': int(data['precipitation_sum'][0]),
            'windspeedkph': int(data['windspeed_10m_max'][0]),
            'weathercondition': weather_code_to_description(data['weathercode'][0])
            
        }

def weather_code_to_description(code):
    weather_codes = {
        0: 'Clear sky', 1: 'Mainly clear', 2: 'Partly cloudy', 3: 'Overcast',
        45: 'Fog', 48: 'Depositing rime fog', 51: 'Light drizzle', 61: 'Slight rain',
        63: 'Moderate rain', 65: 'Heavy rain', 80: 'Rain showers', 95: 'Thunderstorm'
    }
    return weather_codes.get(code, 'Unknown')