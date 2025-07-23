import streamlit as st
import folium
from streamlit_folium import st_folium
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import json
import re

# === Hàm phân loại mức độ cảnh báo và màu ===
def classify_alert(score):
    if score >= 2:
        return "🔴 Nặng", "darkred"
    elif score >= 1:
        return "🟠 Trung bình", "orange"
    elif score >= 0.5:
        return "🟡 Nhẹ", "yellow"
    else:
        return "🟢 Dưới ngưỡng", "lightgreen"

def split_words(name):
    return re.sub(
        r'(?<=[a-zàáảãạăâđèéẻẽẹêìíỉĩịòóỏõọôơùúủũụư])(?=[A-ZÀÁẢÃẠĂÂĐÈÉẺẼẸÊÌÍỈĨỊÒÓỎÕỌÔƠÙÚỦŨỤƯ])',
        ' ', name
    )

# === Load dữ liệu ranh giới và dự đoán ===
gdf = gpd.read_file("yen_bai.geojson")
df = pd.read_csv("yenbai_predictions_clean.csv")

# Load xã trong tỉnh Yên Bái
with open("gadm41_VNM_3.json", encoding="utf-8") as f:
    gadm = json.load(f)
yenbai_xa = [
    feat for feat in gadm["features"]
    if feat["properties"].get("NAME_1", "").lower().replace(" ", "") == "yênbái"
]
gdf_xa = gpd.GeoDataFrame.from_features(yenbai_xa, crs="EPSG:4326")

# Tổng độ ngập theo xã
xa_ngap_dict = {}
for _, row in df.iterrows():
    pt = Point(row["square_center_lon"], row["square_center_lat"])
    for _, xa in gdf_xa.iterrows():
        if xa.geometry.contains(pt):
            name3 = xa["NAME_3"]
            xa_ngap_dict[name3] = xa_ngap_dict.get(name3, 0) + row["pred_flood_score"]

# === Hiển thị bản đồ Streamlit ===
st.set_page_config(layout="wide")
st.title("🌊 Bản đồ dự đoán ngập tại Yên Bái")

center = [df["square_center_lat"].mean(), df["square_center_lon"].mean()]
m = folium.Map(location=center, zoom_start=9, tiles="CartoDB positron")

# Vẽ ranh giới tỉnh
folium.GeoJson(
    gdf,
    name="Yen Bai",
    style_function=lambda x: {"fillColor": "#00000000", "color": "blue", "weight": 2}
).add_to(m)

# Tô màu từng xã
for _, row in gdf_xa.iterrows():
    name3 = row["NAME_3"]
    name_fmt = split_words(name3)
    score = xa_ngap_dict.get(name3, 0)
    alert_level, color = classify_alert(score)
    popup_text = f"{name_fmt}<br>Độ ngập: {score:.1f} ({alert_level})" if score > 0 else name_fmt
    folium.GeoJson(
        row["geometry"],
        name=name_fmt,
        style_function=lambda x, color=color: {
            "fillColor": color,
            "color": "black",
            "weight": 0.5,
            "fillOpacity": 0.7 if color != "#00000000" else 0
        },
        popup=folium.Popup(popup_text, max_width=250)
    ).add_to(m)

st_folium(m, width=800, height=600)

# === Bảng dữ liệu cảnh báo xã ===
records = []
for name3, total_flood_score in xa_ngap_dict.items():
    geom = gdf_xa[gdf_xa["NAME_3"] == name3].geometry.values[0]
    matching_rows = df[
        df.apply(
            lambda row: Point(row["square_center_lon"], row["square_center_lat"]).within(geom),
            axis=1
        )
    ]

    rain_3d_avg = matching_rows["rainfall_3d"].mean()
    rain_7d_avg = matching_rows["rainfall_7d"].mean()
    rain_1m_avg = matching_rows["rainfall_1m"].mean()
    alert_text, _ = classify_alert(total_flood_score)

    records.append({
        "Xã/Thị trấn": split_words(name3),
        "⚠️ Mức độ cảnh báo": alert_text,
        "Tổng độ ngập dự đoán": round(total_flood_score, 2),
        "Mưa TB/ngày (3d)": round(rain_3d_avg ,2),
        "Mưa TB/ngày (7d)": round(rain_7d_avg, 2),
        "Mưa TB/ngày (1m)": round(rain_1m_avg, 2),
    })

df_alert = pd.DataFrame(records)
df_alert = df_alert.sort_values("Tổng độ ngập dự đoán", ascending=False)
st.subheader("📋 Danh sách xã/thị trấn có nguy cơ ngập")
st.dataframe(df_alert, use_container_width=True)


st.subheader("🌧️ Cảnh báo theo lượng mưa (tổng 7 ngày)")

# Tính tổng lượng mưa 7 ngày cho từng xã
xa_rain_dict = {}
for _, row in df.iterrows():
    pt = Point(row["square_center_lon"], row["square_center_lat"])
    for _, xa in gdf_xa.iterrows():
        if xa.geometry.contains(pt):
            name3 = xa["NAME_3"]
            xa_rain_dict[name3] = xa_rain_dict.get(name3, 0) + row["rainfall_7d"]

# Phân loại theo lượng mưa
def classify_rain(rain):
    if rain >= 50:
        return "🔴 Rất lớn", "darkred"
    elif rain >= 40:
        return "🟠 Lớn", "orange"
    elif rain >= 20:
        return "🟡 Vừa", "yellow"
    else:
        return "🟢 Nhỏ", "lightgreen"

# Tạo bản đồ mưa
m_rain = folium.Map(location=center, zoom_start=9, tiles="CartoDB positron")
folium.GeoJson(
    gdf,
    name="Yen Bai",
    style_function=lambda x: {"fillColor": "#00000000", "color": "blue", "weight": 2}
).add_to(m_rain)

for _, row in gdf_xa.iterrows():
    name3 = row["NAME_3"]
    name_fmt = split_words(name3)
    rain_sum = xa_rain_dict.get(name3, 0)
    rain_level, color = classify_rain(rain_sum)
    popup_text = f"{name_fmt}<br>Lượng mưa (7d): {rain_sum:.1f} mm ({rain_level})" if rain_sum > 0 else name_fmt
    folium.GeoJson(
        row["geometry"],
        name=name_fmt,
        style_function=lambda x, color=color: {
            "fillColor": color,
            "color": "black",
            "weight": 0.5,
            "fillOpacity": 0.7 if color != "#00000000" else 0
        },
        popup=folium.Popup(popup_text, max_width=250)
    ).add_to(m_rain)

# Tạo bảng dữ liệu lượng mưa
records_rain = []
for name3, rain_total in xa_rain_dict.items():
    records_rain.append({
        "Xã/Thị trấn": split_words(name3),
        "Lượng mưa (7 ngày, mm)": round(rain_total, 2)
    })
df_rain = pd.DataFrame(records_rain).sort_values("Lượng mưa (7 ngày, mm)", ascending=False)

# Hiển thị 2 cột: bản đồ | bảng
col1, col2 = st.columns([1, 1])
with col1:
    st_folium(m_rain, width=700, height=600)

with col2:
    st.dataframe(df_rain, use_container_width=True)
