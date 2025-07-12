import streamlit as st
import folium
from streamlit_folium import st_folium
import geopandas as gpd
import pandas as pd
import json
import random
from shapely.geometry import Point

# Đọc ranh giới Yên Bái
gdf = gpd.read_file("yen_bai.geojson")

# Đọc các điểm trung tâm ô lớn
df = pd.read_csv("yen_bai_big_squares_centers_wgs84.csv")

# Tạo bản đồ Folium
center = [df["center_lat"].mean(), df["center_lon"].mean()]
m = folium.Map(location=center, zoom_start=9, tiles="CartoDB positron")

# Thêm ranh giới Yên Bái
folium.GeoJson(
    gdf,
    name="Yen Bai",
    style_function=lambda x: {"fillColor": "#00000000", "color": "blue", "weight": 2},
    tooltip=folium.GeoJsonTooltip(fields=["ten_tinh"] if "ten_tinh" in gdf.columns else None)
).add_to(m)

# Thêm các điểm trung tâm ô lớn
for _, row in df.iterrows():
    folium.CircleMarker(
        location=[row["center_lat"], row["center_lon"]],
        radius=4,
        color="red",
        fill=True,
        fill_opacity=0.8,
        popup=row["big_square_id"]
    ).add_to(m)

# Đọc và thêm các huyện Yên Bái vào bản đồ
with open("gadm41_VNM_3.json", encoding="utf-8") as f:
    gadm = json.load(f)

yenbai_huyen = [
    feat for feat in gadm["features"]
    if feat["properties"].get("NAME_1", "").lower().replace(" ", "") == "yênbái"
]

gdf_huyen = gpd.GeoDataFrame.from_features(yenbai_huyen, crs="EPSG:4326")

# Thêm từng huyện với màu khác nhau
for _, row in gdf_huyen.iterrows():
    color = "#{:06x}".format(random.randint(0, 0xFFFFFF))
    folium.GeoJson(
        row["geometry"],
        name=row["NAME_2"],
        style_function=lambda x, color=color: {
            "fillColor": color,
            "color": "black",
            "weight": 1,
            "fillOpacity": 0.3
        },
        tooltip=row["NAME_2"]
    ).add_to(m)

# Đọc dữ liệu cảnh báo (ví dụ: rain_yenbai.csv)
rain_df = pd.read_csv("rain_yenbai.csv")
threshold = 100
rain_df = rain_df[rain_df["rain_5d"] > threshold]

# Xác định huyện nào có điểm cảnh báo
huyen_canh_bao = set()
for _, row_point in rain_df.iterrows():
    pt = Point(row_point["center_lon"], row_point["center_lat"])
    for idx, row_huyen in gdf_huyen.iterrows():
        if row_huyen.geometry.contains(pt):
            huyen_canh_bao.add(row_huyen["NAME_2"])

# Thêm từng huyện: huyện có cảnh báo thì đỏ, còn lại không màu
for _, row in gdf_huyen.iterrows():
    color = "red" if row["NAME_2"] in huyen_canh_bao else "#00000000"
    folium.GeoJson(
        row["geometry"],
        name=row["NAME_2"],
        style_function=lambda x, color=color: {
            "fillColor": color,
            "color": "black",
            "weight": 1,
            "fillOpacity": 0.5 if color == "red" else 0
        },
        tooltip=row["NAME_2"]
    ).add_to(m)

# Hiển thị trên Streamlit
st.title("Bản đồ Yên Bái và các điểm trung tâm ô lớn")
st_folium(m, width=800, height=600)