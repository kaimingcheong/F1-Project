import pandas as pd
import json
from urllib.request import urlopen

def fetch_season_data(year):
    url = f'http://ergast.com/api/f1/{year}/results.json'
    response = urlopen(url)
    data = response.read().decode('utf-8')
    return json.loads(data)

def extract_race_info(race_data):
    race_info = {}
    for race in race_data['MRData']['RaceTable']['Races']:
        circuit_id = race['Circuit']['circuitId']
        race_results = []
        for result in race['Results']:
            race_results.append({
                'grid': result['grid'],
                'position': result['position']
            })
        race_info[circuit_id] = race_results
        return race_info

def fetch_multiple_years(start_year, end_year):
    all_seasons_info = {}
    for year in range(start_year,end_year):
        season_data = fetch_season_data(year)
        race_info = extract_race_info(season_data)
        all_seasons_info[year] = race_info
    return all_seasons_info

all_results = fetch_multiple_years(2008,2022)
for year, season_info in all_results.items():
    print(f"Year: {year}")
    for circuit_id, results in season_info.items():
        print(f"  Circuit ID: {circuit_id}")
        for result in results:
            print(f"    Grid: {result['grid']}, Position: {result['position']}")
    print()