from dagster import asset
import ee
import os
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN

@asset
def water_cluster(context) -> pd.DataFrame:
    # 1. Authenticate and initialize Google Earth Engine
    try:
        ee.Initialize(project='ee-nguyendangkhoi9517')
    except Exception:
        ee.Authenticate()
        ee.Initialize(project='ee-nguyendangkhoi9517')

    # 2. Read combined flood event data
    input_path = "data/intermediate/data_merge.csv"
    if not os.path.exists(input_path):
        context.log.warning("⚠️ Không tìm thấy file combined_flood_event_data.csv.")
        return pd.DataFrame()
    df = pd.read_csv(input_path)
    if df.empty:
        context.log.warning("⚠️ Empty DataFrame for water clustering.")
        return pd.DataFrame()

    # 3. Create GEE geometry and buffer for each point
    df["geometry"] = df.apply(lambda row: ee.Geometry.Point([row["square_center_lon"], row["square_center_lat"]]), axis=1)
    df["buffer"] = df["geometry"].apply(lambda geom: geom.buffer(7000))  # 7km buffer

    # 4. Load permanent water data from JRC Global Surface Water
    water_dataset = ee.Image("JRC/GSW1_4/GlobalSurfaceWater").select("occurrence")
    permanent_water = water_dataset.gte(50)

    # 5. Function to check for permanent water presence in buffer
    def check_water(buffer):
        stats = permanent_water.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=buffer,
            scale=30,
            bestEffort=True
        )
        water_presence = stats.get("occurrence")
        return ee.Algorithms.If(
            water_presence,
            ee.Algorithms.If(ee.Number(water_presence).gt(0), "1", "0"),
            "-1"
        )

    # 6. Apply water presence check for each buffer
    def get_presence(buffer):
        stats = permanent_water.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=buffer,
            scale=30,
            bestEffort=True
        )
        val = stats.get("occurrence")
        if val is None:
            return np.nan
        try:
            return float(val.getInfo())
        except Exception:
            return np.nan

    df["water_presence"] = df["buffer"].apply(get_presence)

    # Cột permanent_water: 1 nếu water_presence > 0, -1 nếu nan hoặc 0
    df["permanent_water"] = df["water_presence"].apply(lambda x: 1 if pd.notnull(x) and x > 0 else -1)

    # 7. Create unique event_square_id
    df["event_square_id"] = df["event_index"].astype(str) + "_" + df["square_index"].astype(str)

    # 8. Cluster valid points using DBSCAN (Haversine distance, ~15km)
    df_valid = df[df["permanent_water"] == 1].copy()
    if not df_valid.empty:
        coords = np.array([[row["square_center_lon"], row["square_center_lat"]] for _, row in df_valid.iterrows()])
        kms_per_radian = 6371.0088
        epsilon = 15 / kms_per_radian
        db = DBSCAN(eps=epsilon, min_samples=1, algorithm='ball_tree', metric='haversine')
        df_valid["water_cluster"] = db.fit(np.radians(coords)).labels_
    else:
        df_valid["water_cluster"] = np.nan

    # Merge cluster results back to original DataFrame
    merge_cols = df_valid[["event_square_id", "water_cluster"]]
    df = df.drop(columns=["water_cluster"], errors="ignore")
    df = df.merge(merge_cols, on="event_square_id", how="left")
    df.drop(columns=["geometry", "buffer"], errors="ignore", inplace=True)

    # Đảm bảo giữ đủ các cột yêu cầu
    keep_cols = [
        "event_index",
        "event_square_id",
        "started_date",
        "ended_date",
        "square_index",
        "square_center_lat",
        "square_center_lon",
        "height_values",
        "flood_values",
        "rainfall_3d",
        "rainfall_7d",
        "rainfall_1m",
        "permanent_water",
        "water_presence",
        "water_cluster"
    ]
    # Chỉ giữ lại các cột có trong DataFrame (tránh lỗi nếu thiếu cột)
    df = df[[col for col in keep_cols if col in df.columns]]

    # 10. Save final results
    output_path = "data/intermediate/water_clusters.csv"
    df.to_csv(output_path, index=False, encoding="utf-8")
    context.log.info(f"✅ Đã lưu cụm điểm với khóa event_square_id tại: {output_path}")
    return df
