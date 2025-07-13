# rainfall_features.py

import ee
import pandas as pd
from datetime import datetime, timedelta
from dagster import asset, Output

# ==== INITIALIZE GEE ====
ee.Initialize(project='ee-nguyendangkhoi9517')

# ==== FUNCTION TO GET PRECIPITATION WITH GPM V07 ====
def get_avg_precip_gpm_v07(lat, lon, end_date, days):
    start = end_date - timedelta(days=days)
    geom = ee.Geometry.Point([lon, lat]).buffer(10000)

    gpm = ee.ImageCollection("NASA/GPM_L3/IMERG_V07") \
        .filterDate(start.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')) \
        .filterBounds(geom)

    try:
        total = gpm.select("precipitationCal").sum()
        mean = total.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=geom,
            scale=10000,
            maxPixels=1e9
        )
        val = mean.getInfo().get("precipitationCal", None)
        if val is None:
            raise ValueError("precipitationCal is null")
        return val
    except:
        try:
            total = gpm.select("precipitationUncal").sum()
            mean = total.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=geom,
                scale=10000,
                maxPixels=1e9
            )
            return mean.getInfo().get("precipitationUncal", 0)
        except:
            return 0

# ==== DAGSTER ASSET ====
@asset
def yenbai_rain() -> Output[pd.DataFrame]:
    print("✅ Loaded latest version of rainfall_features.py")

    input_path = "data/intermediate/yenbai_final.csv"
    output_path = "data/intermediate/yenbai_rainfall.csv"

    today_utc = datetime.utcnow()
    print(f"\n📆 Getting rainfall up to: {today_utc.strftime('%Y-%m-%d')} (UTC)\n")

    df = pd.read_csv(input_path)
    avg_rain_3, avg_rain_7, avg_rain_30 = [], [], []

    for _, row in df.iterrows():
        lat = row['square_center_lat']
        lon = row['square_center_lon']
        square_id = row['big_square_id']

        try:
            rain3 = get_avg_precip_gpm_v07(lat, lon, today_utc, 3)
            rain7 = get_avg_precip_gpm_v07(lat, lon, today_utc, 7)
            rain30 = get_avg_precip_gpm_v07(lat, lon, today_utc, 30)

            avg_rain_3.append(rain3 / 3)
            avg_rain_7.append(rain7 / 7)
            avg_rain_30.append(rain30 / 30)

            print(f"🟢 ID {square_id:<8} | Avg Rain (mm): 3d = {rain3 / 3:.2f}, 7d = {rain7 / 7:.2f}, 30d = {rain30 / 30:.2f}")

        except Exception as e:
            print(f"❌ Error at ID {square_id}: {e}")
            avg_rain_3.append(None)
            avg_rain_7.append(None)
            avg_rain_30.append(None)

    df['rainfall_3d'] = avg_rain_3
    df['rainfall_7d'] = avg_rain_7
    df['rainfall_1m'] = avg_rain_30

    df.to_csv(output_path, index=False)
    print(f"\n✅ Saved results to: {output_path}")

    return Output(value=df)
