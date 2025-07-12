import pandas as pd
import numpy as np
import ee
import os
from dagster import asset, Output

@asset
def fetch_flood_data(context, run_similar: pd.DataFrame) -> Output[None]:
    context.log.info(f"📥 Nhận {len(run_similar)} sự kiện")

    # Khởi tạo Earth Engine
    try:
        ee.Initialize(project='ee-nguyendangkhoi9517')
    except Exception as e:
        context.log.warning("⚠️ Earth Engine đã được khởi tạo trước hoặc lỗi nhỏ: " + str(e))

    # Dữ liệu input
    df = run_similar.copy()
    pending_path = '/data/intermediate/similar_to_yenbai.csv'

    # Nếu file pending tồn tại, chỉ xử lý các event còn lại trong file này
    if os.path.exists(pending_path):
        df = pd.read_csv(pending_path)
        context.log.info(f"🔄 Tiếp tục từ pending: {len(df)} sự kiện.")
    else:
        # Nếu chưa có file pending, tạo mới từ input ban đầu
        df.to_csv(pending_path, index=False)

    # Khai báo nguồn dữ liệu EE
    dem = ee.ImageCollection('COPERNICUS/DEM/GLO30').select('DEM').mosaic()
    chirps = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY').select('precipitation')
    flood_collection = ee.ImageCollection('GLOBAL_FLOOD_DB/MODIS_EVENTS/V1')

    square_size_m = 10000
    small_square_size_m = 250

    def process_event(row):
        center_lon = row["dfo_centroid_x"]
        center_lat = row["dfo_centroid_y"]
        area_km2 = row.get("gfd_area", 100)
        radius = np.sqrt(area_km2) * 1000 / 2

        try:
            began = pd.to_datetime(row["dfo_began"])
            ended = pd.to_datetime(row["dfo_ended"])
        except:
            return []

        began_str = began.strftime("%Y-%m-%d")
        ended_str = ended.strftime("%Y-%m-%d")
        point = ee.Geometry.Point(center_lon, center_lat)
        circle = point.buffer(radius)

        num_squares = int(np.ceil(radius / square_size_m))
        squares = [
            ee.Geometry.Rectangle([
                center_lon + i * square_size_m / 111320 - square_size_m / 111320 / 2,
                center_lat + j * square_size_m / 111320 - square_size_m / 111320 / 2,
                center_lon + i * square_size_m / 111320 + square_size_m / 111320 / 2,
                center_lat + j * square_size_m / 111320 + square_size_m / 111320 / 2
            ])
            for i in range(-num_squares, num_squares + 1)
            for j in range(-num_squares, num_squares + 1)
            if circle.intersects(ee.Geometry.Rectangle([
                center_lon + i * square_size_m / 111320 - square_size_m / 111320 / 2,
                center_lat + j * square_size_m / 111320 - square_size_m / 111320 / 2,
                center_lon + i * square_size_m / 111320 + square_size_m / 111320 / 2,
                center_lat + j * square_size_m / 111320 + square_size_m / 111320 / 2
            ]))
        ]

        rain_3d = chirps.filterDate(began_str, (began + pd.Timedelta(days=3)).strftime("%Y-%m-%d")).mean()
        rain_7d = chirps.filterDate(began_str, (began + pd.Timedelta(days=7)).strftime("%Y-%m-%d")).mean()
        rain_1m = chirps.filterDate(began_str, (began + pd.Timedelta(days=30)).strftime("%Y-%m-%d")).mean()
        flood_img = flood_collection.filterBounds(circle).filterDate(began, ended).mosaic()

        results = []

        for square_idx, square in enumerate(squares):
            try:
                center_coords = square.centroid().getInfo()["coordinates"]
                center_lon_sq, center_lat_sq = center_coords

                rain3 = rain_3d.reduceRegion(ee.Reducer.mean(), square, scale=5000).getInfo().get("precipitation")
                rain7 = rain_7d.reduceRegion(ee.Reducer.mean(), square, scale=5000).getInfo().get("precipitation")
                rain30 = rain_1m.reduceRegion(ee.Reducer.mean(), square, scale=5000).getInfo().get("precipitation")

                height = dem.reduceRegion(ee.Reducer.mean(), square, small_square_size_m).getInfo().get("DEM")
                flood_duration = flood_img.reduceRegion(ee.Reducer.mean(), square, small_square_size_m).getInfo().get("duration")

                results.append({
                    "event_index": row["event_index"],
                    "square_index": square_idx,
                    "square_center_lat": center_lat_sq,
                    "square_center_lon": center_lon_sq,
                    "began_date": began_str,
                    "ended_date": ended_str,
                    "height_value": round(height, 2) if height is not None else None,
                    "flood_duration_value": round(flood_duration, 2) if flood_duration is not None else None,
                    "rainfall_3d": round(rain3, 2) if rain3 is not None else None,
                    "rainfall_7d": round(rain7, 2) if rain7 is not None else None,
                    "rainfall_1m": round(rain30, 2) if rain30 is not None else None
                })

            except Exception as e:
                context.log.warning(f"⚠️ Lỗi xử lý square {square_idx}: {e}")
                continue

        return results

    for idx, row in df.iterrows():
        event_results = process_event(row)
        if not event_results:
            continue

        event_df = pd.DataFrame(event_results)
        event_index = row["event_index"]

        output_path = f"data/intermediate/event_data/data_{event_index}.csv"
        event_df.to_csv(output_path, index=False)
        context.log.info(f"✅ Lưu kết quả: {output_path}")

        # Xóa event khỏi pending
        df = df[df["event_index"] != event_index]
        df.to_csv(pending_path, index=False)
        context.log.info(f"🧹 Đã loại bỏ event {event_index}. Còn lại: {len(df)}")

        # Xóa event khỏi file gốc dbscan_clustering
        original_path = '/data/intermediate/similar_to_yenbai.csv'
        if os.path.exists(original_path):
            original_df = pd.read_csv(original_path)
            original_df = original_df[original_df["event_index"] != event_index]
            original_df.to_csv(original_path, index=False)
            context.log.info(f"🗑️ Đã loại bỏ event {event_index} khỏi file gốc dbscan_clustering.")

    return Output(None)
    context.log.info("✅ Hoàn thành quá trình lấy dữ liệu lũ lụt.")