from dagster import asset
import pandas as pd
import math
import ee

ee.Initialize(project='ee-nguyendangkhoi9517')

def get_avg_elevation_and_rainfall(lat, lon, area_km2, year):
    point = ee.Geometry.Point([lon, lat])
    radius_m = (math.sqrt(area_km2) / 2) * 1000
    buffer = point.buffer(radius_m)

    dem = ee.ImageCollection('COPERNICUS/DEM/GLO30').select('DEM').mosaic()
    elevation_mean = dem.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=buffer,
        scale=90,
        maxPixels=1e8
    ).get('DEM')

    rainfall = (ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY')
                .filterDate(f'{year}-01-01', f'{year}-12-31')
                .filterBounds(buffer)
                .sum())

    rainfall_mean = rainfall.reduceRegion(
        reducer=ee.Reducer.mean(),
        geometry=buffer,
        scale=5000,
        maxPixels=1e8
    ).get('precipitation')

    return ee.Dictionary({
        'elevation': elevation_mean,
        'rainfall': rainfall_mean
    })

@asset
def run_similar(context) -> pd.DataFrame:
    df_raw = pd.read_csv('data/raw/flood_data.csv')

    df_raw = df_raw.rename(columns={
        'dfo_centroid_y': 'latitude',
        'dfo_centroid_x': 'longitude',
        'dfo_began': 'date'
    })

    df_raw['date'] = pd.to_datetime(df_raw['date'], errors='coerce')
    df_raw = df_raw.dropna(subset=['date'])
    df_raw['year'] = df_raw['date'].dt.year

    flood_points = df_raw[['latitude', 'longitude', 'year', 'gfd_area']].dropna().reset_index()

    results = []
    for i, row in flood_points.iterrows():
        try:
            stats = get_avg_elevation_and_rainfall(
                row['latitude'], row['longitude'], row['gfd_area'], int(row['year'])
            ).getInfo()

            results.append({
                'index': row['index'],  # để merge ngược lại với df_raw
                'elevation': stats['elevation'],
                'rainfall': stats['rainfall']
            })
        except Exception as e:
            context.log.warn(f"Lỗi dòng {row['index']}: {e}")

    df_stats = pd.DataFrame(results)

    # Gộp elevation + rainfall vào lại df_raw
    df_full = df_raw.reset_index().merge(df_stats, on='index', how='inner').drop(columns='index')

    # Lọc theo điều kiện giống Yên Bái
    df_similar = df_full[
        (df_full['elevation'] >= 300) & (df_full['elevation'] <= 700) &
        (df_full['rainfall'] >= 1400) & (df_full['rainfall'] <= 2000)
    ]

    # Ghi ra file
    output_path = '/data/intermediate/similar_to_yenbai.csv'
    df_similar.to_csv(output_path, index=False)

    context.log.info(f"Lưu {len(df_similar)} dòng giống Yên Bái vào: {output_path}")
    return df_similar
