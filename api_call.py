import fastf1
import pandas as pd

# Enable cache
fastf1.Cache.enable_cache('cache')

#Extracting session data
def extract_session_data(session):
    session_data = []

    for lap in session.laps.iterlaps():
        session_data.append({
            'Year': session.date.year,
            'Grand Prix': session.event['EventName'],
            'Session': session.session_name,
            'Driver': lap.Driver,
            'Start Position': lap['Position'],
            'Finish Position': lap['Position'],
            'Final Grid': lap['GridPosition'],
            'Lap Number': lap['LapNumber'],
            'Lap Time': lap['LapTime'],
            'Sector 1 Time': lap['Sector1Time'],
            'Sector 2 Time': lap['Sector2Time'],
            'Sector 3 Time': lap['Sector3Time'],
            'Pit Stop Count': len(lap['PitInLap']),
            'Pit Stop Duration': lap['PitStopTime'],
            'Tyre Compound': lap['Compound'],
            'Track Temperature': session.weather_data['TrackTemp'].mean(),
            'Air Temperature': session.weather_data['AirTemp'].mean(),
            'Weather Changes': session.weather_data['Weather'].unique(),
            'Track Layout': session.track_layout,
            'Track Length': session.track_length,
            'Configuration': session.configuration
        })

    return pd.DataFrame(session_data)

#Extract weekend data
def extract_race_weekend_data(year, grand_prix):
    sessions = ['FP1', 'FP2', 'FP3', 'Q', 'R']
    weekend_data = []
    metadata = []

    for session_name in sessions:
        try:
            session = fastf1.get_session(year, grand_prix, session_name)
            session.load()  # Necessary to fetch the detailed data
            session_df = extract_session_data(session)
            weekend_data.append(session_df)

            metadata.append({
                'Year': year,
                'Grand Prix': grand_prix,
                'Session': session_name,
                'Number of Laps': len(session.laps),
                'Weather Summary': session.weather_data['Weather'].unique()
            })

            print(f"Data extracted for {year} {grand_prix} {session_name}")

        except Exception as e:
            print(f"Error with {year} {grand_prix} {session_name}: {e}")

    all_session_data = pd.concat(weekend_data, ignore_index=True)
    metadata_df = pd.DataFrame(metadata)

    return all_session_data, metadata_df

#Extracting year range
def extract_data_for_range(start_year, end_year):
    all_data = []
    metadata_list = []

    for year in range(start_year, end_year + 1):
        schedule = fastf1.get_event_schedule(year)

        for gp in schedule['EventName']:
            try:
                df, metadata = extract_race_weekend_data(year, gp)
                all_data.append(df)
                metadata_list.append(metadata)
                print(f"Data extracted for {year} {gp}")
            except Exception as e:
                print(f"Error with {year} {gp}: {e}")

    all_race_data = pd.concat(all_data, ignore_index=True)
    metadata_df = pd.DataFrame(metadata_list)

    return all_race_data, metadata_df

race_data_range, metadata = extract_data_for_range(2018, 2023)

# Save race data and metadata to CSV files
race_data_range.to_csv('f1_race_data_2018_2023.csv', index=False)
metadata.to_csv('f1_race_metadata_2018_2023.csv', index=False)