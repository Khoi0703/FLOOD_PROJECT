import json
import streamlit as st

@st.cache_resource
def load_geojson():
    with open("mnt/data/diaphantinh.geojson", "r", encoding="utf-8") as f:
        return json.load(f)

def get_province_coordinates(province_name, geojson_data):
    for feature in geojson_data["features"]:
        if feature["properties"]["ten_tinh"] == province_name:
            coords = feature["geometry"]["coordinates"]
            if isinstance(coords[0][0][0], list):  
                lon, lat = coords[0][0][0]  
            else:
                lon, lat = coords[0][0]  
            return lat, lon
    return None, None
