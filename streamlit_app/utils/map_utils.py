import folium
from folium.plugins import MarkerCluster
from .geojson_utils import get_province_coordinates

def create_map(geojson_data, weather_data, rain_threshold=100):
    """
    Tạo bản đồ với các khu vực được tô màu dựa trên lượng mưa.
    
    Args:
        geojson_data (dict): Dữ liệu GeoJSON của Việt Nam.
        weather_data (dict): Dữ liệu thời tiết cho từng tỉnh.
        rain_threshold (int): Ngưỡng lượng mưa để cảnh báo (mm).
    
    Returns:
        folium.Map: Đối tượng bản đồ Folium.
    """
    # Tạo bản đồ trung tâm tại Việt Nam
    m = folium.Map(location=[16.0, 108.0], zoom_start=6)

    # Thêm dữ liệu GeoJSON vào bản đồ
    def style_function(feature):
        province_name = feature["properties"]["ten_tinh"]
        if province_name in weather_data:
            total_rain = weather_data[province_name]["total_rain"]
            # Đổi màu dựa trên lượng mưa
            if total_rain > rain_threshold:
                return {"fillColor": "red", "color": "black", "weight": 1, "fillOpacity": 0.7}
            else:
                return {"fillColor": "blue", "color": "black", "weight": 1, "fillOpacity": 0.5}
        return {"fillColor": "gray", "color": "black", "weight": 1, "fillOpacity": 0.3}

    folium.GeoJson(
        geojson_data,
        style_function=style_function,
        tooltip=folium.GeoJsonTooltip(
            fields=["ten_tinh"],
            aliases=["Tỉnh/Thành phố:"],
            localize=True
        )
    ).add_to(m)

    return m
