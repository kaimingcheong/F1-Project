import fastf1
import logging

# Set up logging to suppress INFO messages
logging.basicConfig(level=logging.WARNING)

# Enable cache
fastf1.Cache.enable_cache('cache')

def load_and_save_session_data(year, grand_prix, session_name='FP1'):
    try:
        session = fastf1.get_session(year, grand_prix, session_name)
        session.load()

        # Print basic session information
        print(f"Session: {session_name} for {grand_prix} in {year}")
        print(f"Loaded drivers: {session.drivers}")

        # Check if laps data is available and save it to CSV
        if not session.laps.empty:
            print("Lap data loaded successfully.")
            print(session.laps.head())

            # Save lap data to CSV
            session.laps.to_csv(f'{year}_{grand_prix}_{session_name}_lap_data.csv', index=False)
            print(f"Lap data saved to {year}_{grand_prix}_{session_name}_lap_data.csv")
        else:
            print("No lap data available for this session.")

    except Exception as e:
        print(f"Error loading session data: {e}")

# Debugging for the first race of 2018
year = 2018
grand_prix = 'Australian Grand Prix'

load_and_save_session_data(year, grand_prix)