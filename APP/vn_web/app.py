import streamlit as st
from streamlit_folium import st_folium
from utils.map_utils import create_map
from utils.geojson_utils import load_geojson
import pandas as pd
import json
import os
from assets.weather_assets import vietnam_geojson, province_coordinates, weather_forecasts

st.title("📍 Bản đồ dự báo thời tiết Việt Nam")

# Load dữ liệu địa giới hành chính Việt Nam
geojson_data = vietnam_geojson()
province_coords = province_coordinates(geojson_data)
weather_data = weather_forecasts(province_coords)

RAIN_THRESHOLD = 100  # mm trong 5 ngày

# Tạo bản đồ
if geojson_data and weather_data:
    map_object = create_map(geojson_data, weather_data, rain_threshold=RAIN_THRESHOLD)
    st_data = st_folium(map_object, width=700, height=500)

    # Cảnh báo lượng mưa cao
    high_rain_provinces = [
        province for province, data in weather_data.items()
        if data.get("total_rain", 0) > RAIN_THRESHOLD
    ]
    if high_rain_provinces:
        st.warning("⚠️ **Cảnh báo các khu vực có lượng mưa cao:**")
        for province in high_rain_provinces:
            st.markdown(f"- **{province}**: {weather_data[province]['total_rain']} mm")
    else:
        st.success("✅ Không có khu vực nào vượt ngưỡng lượng mưa cao.")

    # Xử lý chọn tỉnh/thành phố từ bản đồ hoặc selectbox
    province_list = [f["properties"]["ten_tinh"] for f in geojson_data["features"]]
    if "selected_province" not in st.session_state:
        st.session_state.selected_province = None

    if st_data and st_data.get("last_active_drawing"):
        properties = st_data["last_active_drawing"]["properties"]
        province_name = properties.get("ten_tinh")
        if province_name in province_list:
            st.session_state.selected_province = province_name

    selected_province = st.selectbox(
        "🔍 Chọn tỉnh/thành phố:",
        province_list,
        index=province_list.index(st.session_state.selected_province) if st.session_state.selected_province in province_list else 0
    )
    if selected_province != st.session_state.selected_province:
        st.session_state.selected_province = selected_province

    # Hiển thị dự báo chi tiết khi nhấn nút
    if st.button("📡 Xem dự báo thời tiết"):
        province_weather = weather_data.get(st.session_state.selected_province)
        if province_weather and "forecast" in province_weather:
            forecast = province_weather["forecast"]
            # Đảm bảo forecast là DataFrame
            if not isinstance(forecast, pd.DataFrame):
                try:
                    forecast = pd.DataFrame(forecast)
                except Exception:
                    st.error("❌ Không thể chuyển đổi dữ liệu dự báo sang DataFrame.")
                    forecast = None
            if forecast is not None:
                with st.expander(f"📍 Dự báo thời tiết tại {st.session_state.selected_province}", expanded=True):
                    st.subheader("📅 **Dự báo thời tiết 5 ngày**")
                    st.dataframe(forecast)
                    st.markdown(f"### 🌧️ **Tổng lượng mưa trong 5 ngày**: `{province_weather['total_rain']} mm`")
                    st.info("ℹ️ Dữ liệu có thể được lấy từ cache để tối ưu hóa.")
        else:
            st.error("❌ Không có dữ liệu thời tiết cho tỉnh/thành phố này!")

    # Hàm lưu dữ liệu thời tiết ra CSV
    def save_weather_data_to_csv(weather_data, csv_file):
        data = []
        for province, info in weather_data.items():
            forecast = info.get("forecast")
            if forecast is not None:
                # Đảm bảo forecast là DataFrame
                if not isinstance(forecast, pd.DataFrame):
                    try:
                        forecast = pd.DataFrame(forecast)
                    except Exception:
                        continue
                for _, row in forecast.iterrows():
                    data.append({
                        "Tỉnh/Thành phố": province,
                        "Ngày": row.get("📅 Ngày", "N/A"),
                        "Nhiệt độ trung bình (°C)": row.get("🌡️ Nhiệt độ trung bình (°C)", "N/A"),
                        "Độ ẩm trung bình (%)": row.get("💧 Độ ẩm trung bình (%)", "N/A"),
                        "Gió trung bình (m/s)": row.get("🌬️ Gió trung bình (m/s)", "N/A"),
                        "Tổng lượng mưa (mm)": row.get("🌧️ Tổng lượng mưa (mm)", "N/A")
                    })
        if data:
            df = pd.DataFrame(data)
            df.to_csv(csv_file, index=False, encoding="utf-8-sig")
            st.success(f"Dữ liệu thời tiết đã được lưu vào file `{csv_file}`")
        else:
            st.error("Không có dữ liệu hợp lệ để lưu vào CSV.")

    # Nút lưu dữ liệu ra CSV
    CSV_FILE = "weather_data.csv"
    if st.button("💾 Lưu toàn bộ dữ liệu thời tiết ra CSV"):
        save_weather_data_to_csv(weather_data, CSV_FILE)
else:
    st.error("Không đủ dữ liệu để hiển thị bản đồ hoặc dự báo thời tiết.")
