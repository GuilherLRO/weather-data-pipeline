import requests
import datetime
import pandas as pd
import uuid
import datetime

api_key = "695cc27371e94dbca45172359232412"
base_url = "http://api.weatherapi.com/v1"
save_path = ""

def get_current_weather(api_key, location):
    base_url = "http://api.weatherapi.com/v1"
    current_weather_url = base_url + "/current.json"
    params = {"key": api_key, "q": location}
    response = requests.get(current_weather_url, params=params)
    return response.json()

def get_history_weather(api_key, location, date):
    base_url = "http://api.weatherapi.com/v1"
    history_weather_url = base_url + "/history.json"
    params = {"key": api_key, "q": location, "dt": date}
    response = requests.get(history_weather_url, params=params)
    return response.json()

def get_forecast_weather(api_key, location, days):
    base_url = "http://api.weatherapi.com/v1"
    forecast_weather_url = base_url + "/forecast.json"
    params = {"key": api_key, "q": location, "days": days}
    response = requests.get(forecast_weather_url, params=params)
    return response.json()

def get_cities_to_weather():
    brasilian_states   = [ 'Acre', 'Alagoas', 'Amapá', 'Amazonas', 'Bahia', 'Ceará', 'Espírito Santo', 'Goiás', 'Maranhão', 'Mato Grosso', 'Mato Grosso do Sul', 'Minas Gerais', 'Pará', 'Paraíba', 'Paraná', 'Pernambuco', 'Piauí', 'Rio de Janeiro', 'Rio Grande do Norte', 'Rio Grande do Sul', 'Rondônia', 'Roraima', 'Santa Catarina', 'São Paulo', 'Sergipe', 'Tocantins' ]
    brasilian_capitals = [ 'Rio Branco', 'Maceió', 'Macapá', 'Manaus', 'Salvador', 'Fortaleza',  'Vitória', 'Goiânia', 'São Luís', 'Cuiabá', 'Campo Grande', 'Belo Horizonte', 'Belém', 'João Pessoa', 'Curitiba', 'Recife', 'Teresina', 'Rio de Janeiro', 'Natal', 'Porto Alegre', 'Porto Velho', 'Boa Vista', 'Florianópolis', 'São Paulo', 'Aracaju', 'Palmas' ]
    cities = [ x + ", " + y + ', Brazil' for x, y in zip(brasilian_capitals, brasilian_states) ]
    cities_unaccented = [ x.replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u').replace('ã', 'a').replace('õ', 'o').replace('ç', 'c') for x in cities]
    cities_upper = [ x.upper() for x in cities_unaccented ]
    cities = cities_upper
    return cities

def get_last_3days():
    today = datetime.datetime.now()
    last_3days = [ (today - datetime.timedelta(days=x)).strftime("%Y-%m-%d") for x in range(3) ]
    return last_3days

def create_current_weather_file(cities,save_path = save_path):
    current_df = pd.DataFrame()
    infos = ['query']
    for a in cities:
        print(a)
        record_id = uuid.uuid4()

        current_weather = get_current_weather(api_key, a)
        for type in current_weather.keys():
            for info in current_weather[type]:
                value = current_weather[type][info]
                if info not in infos:
                    infos.append(info)
                current_df = pd.concat([current_df, pd.DataFrame({'record_id': [record_id], 'type': [type], 'info': [info], 'value': [value]})])
        query_info = {'record_id': [record_id], 'type': 'location', 'info': 'query', 'value': a}
        current_df = pd.concat([current_df, pd.DataFrame(query_info)])

    current_data_raw = current_df.pivot(index='record_id', columns='info', values='value').reset_index()[infos]
    current_data_raw.to_csv(save_path + 'current_raw.csv', index=False)
    current_data_raw.head()

def create_forecast_weather_file(cities, save_path = save_path):
    current_df = pd.DataFrame()
    infos = ['query']
    for a in cities:
        print(a)
        record_id = uuid.uuid4()
        forecast_weather = get_forecast_weather(api_key, a, 3)
        for type in forecast_weather.keys():
            for info in forecast_weather[type]:
                value = forecast_weather[type][info]
                if info not in infos:
                    infos.append(info)
                current_df = pd.concat([current_df, pd.DataFrame({'record_id': [record_id], 'type': [type], 'info': [info], 'value': [value]})])
        query_info = {'record_id': [record_id], 'type': 'location', 'info': 'query', 'value': a}
        current_df = pd.concat([current_df, pd.DataFrame(query_info)])
        
    forecast_raw = current_df.pivot(index='record_id', columns='info', values='value').reset_index()[infos]

    df = forecast_raw.explode('forecastday')
    df['date'] = df['forecastday'].apply(lambda x: x['date'])
    df['day'] = df['forecastday'].apply(lambda x: x['day'])
    df['hour'] = df['forecastday'].apply(lambda x: x['hour'])
    df.to_csv(save_path + 'forecast_raw.csv', index=False)

def create_historical_weather_file(cities,historic_weather_days = get_last_3days(), save_path = save_path):
    current_df = pd.DataFrame()
    infos = ['query']
    days = historic_weather_days
    for day in days:
        for a in cities:
            print(day + " - " + a)
            record_id = uuid.uuid4()
            history_weather = get_history_weather(api_key, a, day)
            for type in history_weather.keys():
                for info in history_weather[type]:
                    value = history_weather[type][info]
                    if info not in infos:
                        infos.append(info)
                    current_df = pd.concat([current_df, pd.DataFrame({'record_id': [record_id], 'type': [type], 'info': [info], 'value': [value]})])
            query_info = {'record_id': [record_id], 'type': 'location', 'info': 'query', 'value': a}
            current_df = pd.concat([current_df, pd.DataFrame(query_info)])
            
    history_raw = current_df.pivot(index='record_id', columns='info', values='value').reset_index()


    df = history_raw.explode('forecastday')
    df['date'] = df['forecastday'].apply(lambda x: x['date'])
    df['day'] = df['forecastday'].apply(lambda x: x['day'])
    df['hour'] = df['forecastday'].apply(lambda x: x['hour'])
    df.to_csv(save_path + 'history_raw.csv', index=False)

aaaa = "x"