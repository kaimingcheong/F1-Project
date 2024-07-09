import fastf1
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Enable cache
fastf1.Cache.enable_cache('cache')

# Extracting session data
def extract_session_data(session):
    session_data = []

    for index, lap in session.laps.iterlaps():
        session_data.append({
            'Year': session.date.year,
            'Grand Prix': session.event['EventName'],
            'Session': session.name,  # Correct attribute for session name
            'Driver': lap['Driver'],
            'Driver Number': lap['DriverNumber'] if 'DriverNumber' in lap else pd.NA,
            'Start Position': lap['Position'] if 'Position' in lap else pd.NA,
            'Finish Position': lap['Position'] if 'Position' in lap else pd.NA,
            'Final Grid': lap.get('GridPosition', pd.NA),
            'Lap Number': lap['LapNumber'],
            'Lap Time': lap['LapTime'],
            'Sector 1 Time': lap['Sector1Time'],
            'Sector 2 Time': lap['Sector2Time'],
            'Sector 3 Time': lap['Sector3Time'],
            'Pit Stop Count': len(lap['PitInLap']) if 'PitInLap' in lap else 0,
            'Pit Stop Duration': lap.get('PitStopTime', pd.NaT),
            'Tyre Compound': lap['Compound'],
            'Track Temperature': session.weather_data['TrackTemp'].mean(),
            'Air Temperature': session.weather_data['AirTemp'].mean(),
            'Weather Changes': ", ".join(session.weather_data['Weather'].unique()) if 'Weather' in session.weather_data else pd.NA,
            'Track Layout': getattr(session, 'track_layout', pd.NA),
            'Track Length': getattr(session, 'track_length', pd.NA),
            'Configuration': getattr(session, 'configuration', pd.NA)
        })

    return pd.DataFrame(session_data)

# Extract weekend data
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
                'Weather Summary': ", ".join(session.weather_data['Weather'].unique()) if 'Weather' in session.weather_data else pd.NA
            })

            print(f"Data extracted for {year} {grand_prix} {session_name}")

        except Exception as e:
            print(f"Error with {year} {grand_prix} {session_name}: {e}")

    if weekend_data:
        all_session_data = pd.concat(weekend_data, ignore_index=True)
    else:
        all_session_data = pd.DataFrame()

    if metadata:
        metadata_df = pd.DataFrame(metadata)
    else:
        metadata_df = pd.DataFrame()

    return all_session_data, metadata_df

def extract_data_for_range(start_year, end_year):
    all_data = []
    metadata_list = []

    def worker(year, gp):
        try:
            return extract_race_weekend_data(year, gp)
        except Exception as e:
            print(f"Error with {year} {gp}: {e}")
            return pd.DataFrame(), []

    schedule = []
    for year in range(start_year, end_year + 1):
        events = fastf1.get_event_schedule(year)
        for gp in events['EventName']:
            schedule.append((year, gp))

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_event = {executor.submit(worker, year, gp): (year, gp) for year, gp in schedule}
        for future in as_completed(future_to_event):
            year, gp = future_to_event[future]
            try:
                df, metadata = future.result()
                all_data.append(df)
                metadata_list.append(metadata)
                print(f"Data extracted for {year} {gp}")
            except Exception as e:
                print(f"Error processing {year} {gp}: {e}")

    all_race_data = pd.concat(all_data, ignore_index=True)
    metadata_df = pd.concat(metadata_list, ignore_index=True)

    return all_race_data, metadata_df

#Extract data across a range of years
race_data_range, metadata = extract_data_for_range(2023, 2023)

# Save race data and metadata to CSV files
race_data_range.to_csv('f1_race_data_2023.csv', index=False)
metadata.to_csv('f1_race_metadata_2023.csv', index=False)