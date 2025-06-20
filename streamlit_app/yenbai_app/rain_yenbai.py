import streamlit as st
import folium
from streamlit_folium import st_folium
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import json
import re

# Đọc ranh giới tỉnh Yên Bái
gdf = gpd.read_file("yen_bai.geojson")

# Đọc kết quả dự đoán
df = pd.read_csv("yenbai_predictions_clean.csv")

# Ngưỡng cảnh báo độ ngập
threshold = 50
df = df[df["pred_flood_score"] > threshold]

# Đọc dữ liệu hành chính cấp xã
with open("gadm41_VNM_3.json", encoding="utf-8") as f:
    gadm = json.load(f)
yenbai_xa = [
    feat for feat in gadm["features"]
    if feat["properties"].get("NAME_1", "").lower().replace(" ", "") == "yênbái"
]
gdf_xa = gpd.GeoDataFrame.from_features(yenbai_xa, crs="EPSG:4326")

# Xác định xã có điểm dự đoán ngập cao
xa_ngap_cao = set()
xa_ngap_dict = {}
for _, row in df.iterrows():
    pt = Point(row["square_center_lon"], row["square_center_lat"])
    for _, xa in gdf_xa.iterrows():
        if xa.geometry.contains(pt):
            name3 = xa["NAME_3"]
            xa_ngap_cao.add(name3)
            xa_ngap_dict[name3] = xa_ngap_dict.get(name3, 0) + row["pred_flood_score"]

def split_words(name):
    return re.sub(
        r'(?<=[a-zàáảãạăâđèéẻẽẹêìíỉĩịòóỏõọôơùúủũụư])(?=[A-ZÀÁẢÃẠĂÂĐÈÉẺẼẸÊÌÍỈĨỊÒÓỎÕỌÔƠÙÚỦŨỤƯ])',
        ' ', name
    )

# Vẽ bản đồ
st.set_page_config(layout="wide")
st.title("🌊 Bản đồ dự đoán ngập tại Yên Bái")
if df.empty:
    st.warning("Không có khu vực nào có dự đoán ngập vượt ngưỡng.")
else:
    center = [df["square_center_lat"].mean(), df["square_center_lon"].mean()]
    m = folium.Map(location=center, zoom_start=9, tiles="CartoDB positron")

    # Thêm ranh giới tỉnh
    folium.GeoJson(
        gdf,
        name="Yen Bai",
        style_function=lambda x: {"fillColor": "#00000000", "color": "blue", "weight": 2}
    ).add_to(m)

    # Thêm từng xã
    for _, row in gdf_xa.iterrows():
        name3 = row["NAME_3"]
        name_fmt = split_words(name3)
        color = "red" if name3 in xa_ngap_cao else "#00000000"
        flood_score = xa_ngap_dict.get(name3, 0)
        popup_text = f"{name_fmt}<br>Dự đoán độ ngập: {flood_score:.1f}" if name3 in xa_ngap_dict else name_fmt
        folium.GeoJson(
            row["geometry"],
            name=name_fmt,
            style_function=lambda x, color=color: {
                "fillColor": color,
                "color": "black",
                "weight": 0.5,
                "fillOpacity": 0.7 if color == "red" else 0
            },
            popup=folium.Popup(popup_text, max_width=250)
        ).add_to(m)

    # Hiển thị bản đồ
    st_folium(m, width=800, height=600)

    # Hiển thị bảng dữ liệu
    if xa_ngap_dict:
        records = []
        for name3, total_flood_score in xa_ngap_dict.items():
            matching_rows = df[
                df.apply(
                    lambda row: any(
                        row["square_center_lat"] == pt.y and row["square_center_lon"] == pt.x
                        and row["pred_flood_score"] > threshold
                        and Point(row["square_center_lon"], row["square_center_lat"]).within(geom)
                        for geom in gdf_xa[gdf_xa["NAME_3"] == name3].geometry
                        for pt in [Point(row["square_center_lon"], row["square_center_lat"])]
                    ),
                    axis=1
                )
            ]

            # Tính TRUNG BÌNH lượng mưa
            rain_3d_avg = matching_rows["rainfall_3d"].mean()
            rain_7d_avg = matching_rows["rainfall_7d"].mean()
            rain_1m_avg = matching_rows["rainfall_1m"].mean()

            records.append({
                "Xã/Thị trấn": split_words(name3),
                "Tổng độ ngập dự đoán": total_flood_score,
                "Mưa TB/ngày (3d)": round(rain_3d_avg / 3, 2),
                "Mưa TB/ngày (7d)": round(rain_7d_avg / 7, 2),
                "Mưa TB/ngày (1m)": round(rain_1m_avg / 30, 2),
            })

        df_alert = pd.DataFrame(records)
        df_alert = df_alert.sort_values("Tổng độ ngập dự đoán", ascending=False)
        st.subheader("📋 Danh sách xã/thị trấn có nguy cơ ngập cao")
        st.dataframe(df_alert, use_container_width=True)


