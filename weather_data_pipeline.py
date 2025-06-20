import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import joblib
import os

# ---------------------------
# Configuration
# ---------------------------
LAT = 0.5143
LON = 35.2698
TIMEZONE = "Africa/Nairobi"
DATA_COLUMNS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max"
]
CSV_FILENAME = "uasin_gishu_weather_data_cleaned.csv"
PIPELINE_FILENAME = "weather_data_pipeline.joblib"
LOG_FILENAME = "weather_pipeline.log"

# ---------------------------
# Logging Setup
# ---------------------------
logging.basicConfig(
    filename=LOG_FILENAME,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ---------------------------
# Weather Data Pipeline Class
# ---------------------------
class WeatherDataPipeline:
    def __init__(self, lat, lon, timezone, columns):
        self.lat = lat
        self.lon = lon
        self.columns = columns
        self.timezone = timezone
        self.df = None

    def get_date_range(self, years=5):
        end_date = datetime.today().date()
        start_date = end_date - timedelta(days=years * 365)
        return start_date.isoformat(), end_date.isoformat()

    def build_url(self, start, end):
        daily_params = ",".join(self.columns)
        return (
            f"https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={self.lat}&longitude={self.lon}"
            f"&start_date={start}&end_date={end}"
            f"&daily={daily_params}"
            f"&timezone={self.timezone.replace('/', '%2F')}"
        )

    def fetch_data(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Error fetching data: {e}")
            raise

    def transform_to_dataframe(self, data):
        if "daily" not in data:
            raise ValueError("Missing 'daily' key in API response.")
        self.df = pd.DataFrame(data["daily"])
        self.df["time"] = pd.to_datetime(self.df["time"])

    def handle_missing_values(self):
        if self.df.isna().sum().any():
            logging.warning("Missing values found:\n" + str(self.df.isna().sum()))
            self.df.fillna(self.df.mean(numeric_only=True), inplace=True)
            logging.info("Filled missing values with column means.")

    def save_data(self, filename=CSV_FILENAME):
        self.df.to_csv(filename, index=False)
        logging.info(f"Saved cleaned data to {filename}")

    def run(self):
        logging.info("🚀 Starting pipeline...")
        start, end = self.get_date_range()
        url = self.build_url(start, end)
        data = self.fetch_data(url)
        self.transform_to_dataframe(data)
        self.handle_missing_values()
        self.save_data()
        logging.info("✅ Pipeline completed successfully.")

# ---------------------------
# Run & Save Pipeline
# ---------------------------
def run_and_save_pipeline():
    try:
        pipeline = WeatherDataPipeline(LAT, LON, TIMEZONE, DATA_COLUMNS)
        pipeline.run()

        # Save pipeline object
        joblib.dump(pipeline, PIPELINE_FILENAME)
        logging.info(f"💾 Pipeline object saved as {PIPELINE_FILENAME}")
        print(f"✅ Pipeline run and saved to {PIPELINE_FILENAME}")

    except Exception as e:
        logging.error(f"❌ Pipeline failed: {e}")
        print(f"Pipeline failed: {e}")

# ---------------------------
# Entry Point
# ---------------------------
if __name__ == "__main__":
    run_and_save_pipeline()
