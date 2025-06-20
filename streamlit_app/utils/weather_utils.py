import requests
import pandas as pd
import streamlit as st

API_KEY = "83e31c8156394ec11a34172b6627e920"

def get_weather_forecast(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&units=metric&appid={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if "list" not in data:
            st.error("Không tìm thấy dữ liệu dự báo!")
            return None, 0  

        daily_forecast = {}
        total_rain = 0  

        for entry in data["list"]:
            date = pd.to_datetime(entry["dt"], unit="s").strftime("%d-%m-%Y")
            temp = entry["main"]["temp"]
            humidity = entry["main"]["humidity"]
            wind_speed = entry["wind"]["speed"]
            rain = entry.get("rain", {}).get("3h", 0)  
            description = entry["weather"][0]["description"].capitalize()

            if date not in daily_forecast:
                daily_forecast[date] = {
                    "🌡️ Nhiệt độ (°C)": [],
                    "💧 Độ ẩm (%)": [],
                    "🌬️ Gió (m/s)": [],
                    "🌧️ Lượng mưa (mm)": []
                }

            daily_forecast[date]["🌡️ Nhiệt độ (°C)"].append(temp)
            daily_forecast[date]["💧 Độ ẩm (%)"].append(humidity)
            daily_forecast[date]["🌬️ Gió (m/s)"].append(wind_speed)
            daily_forecast[date]["🌧️ Lượng mưa (mm)"].append(rain)

            total_rain += rain  

        forecast = []
        for date, values in daily_forecast.items():
            forecast.append({
                "📅 Ngày": date,
                "🌡️ Nhiệt độ trung bình (°C)": sum(values["🌡️ Nhiệt độ (°C)"]) / len(values["🌡️ Nhiệt độ (°C)"]),
                "💧 Độ ẩm trung bình (%)": sum(values["💧 Độ ẩm (%)"]) / len(values["💧 Độ ẩm (%)"]),
                "🌬️ Gió trung bình (m/s)": sum(values["🌬️ Gió (m/s)"]) / len(values["🌬️ Gió (m/s)"]),
                "🌧️ Tổng lượng mưa (mm)": sum(values["🌧️ Lượng mưa (mm)"])
            })

        return forecast, total_rain

    except requests.exceptions.RequestException as e:
        st.error(f"Lỗi khi gọi API: {e}")
        return None, 0
