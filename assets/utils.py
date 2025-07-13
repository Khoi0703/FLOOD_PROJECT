import numpy as np
import pandas as pd
import ast
import torch
import random
import os
from dagster import asset

def parse_list_string_to_2d_array(s, img_height, img_width):
    """
    Convert a string like '[0.1, 0.2, ...]' to a 2D numpy array (img_height, img_width).
    If error or wrong shape, return a zero array.
    """
    flat_list_len = img_height * img_width
    if isinstance(s, (list, np.ndarray)):
        arr = np.array(s, dtype=np.float32)
        if arr.ndim == 2 and arr.shape == (img_height, img_width): return arr
        elif arr.ndim == 1 and len(arr) == flat_list_len: return arr.reshape(img_height, img_width)
        else: return np.zeros((img_height, img_width), dtype=np.float32)
    if not isinstance(s, str): return np.zeros((img_height, img_width), dtype=np.float32)
    try:
        arr = np.array(ast.literal_eval(s), dtype=np.float32)
        if len(arr) == flat_list_len: return arr.reshape(img_height, img_width)
        else: return np.zeros((img_height, img_width), dtype=np.float32)
    except (ValueError, SyntaxError): return np.zeros((img_height, img_width), dtype=np.float32)

def create_dummy_data(img_height, img_width, num_events=3, squares_per_event_range=(5, 10), num_water_clusters=5):
    """
    Generate dummy data for the flood dataset, used for debugging or pipeline demo.
    """
    data = []
    for event_idx in range(num_events):
        num_squares_in_event = np.random.randint(squares_per_event_range[0], squares_per_event_range[1])
        began_date_dt = pd.to_datetime('2000-01-01') + pd.to_timedelta(np.random.randint(0, 3650), unit='D')
        ended_date_dt = began_date_dt + pd.to_timedelta(np.random.randint(1, 30), unit='D')
        for square_idx in range(num_squares_in_event):
            permanent_water_val = np.random.choice([1, -1])
            water_presence_val = np.random.rand() * 0.1 if permanent_water_val == 1 else np.nan
            water_cluster_val = np.random.randint(0, num_water_clusters) if permanent_water_val == 1 else np.nan
            data.append({
                'event_index': event_idx, 'square_index': square_idx,
                'event_square_id': f"{event_idx}_{square_idx}",
                'began_date': began_date_dt.strftime('%m/%d/%Y'),
                'ended_date': ended_date_dt.strftime('%m/%d/%Y'),
                'square_center_lat': -30 + np.random.rand() * 2,
                'square_center_lon': 140 + np.random.rand() * 2,
                'height_values': str(np.random.rand(img_height * img_width).tolist()),
                'flood_values': str(np.random.rand(img_height * img_width).tolist()),
                'rainfall_3d': np.random.rand() * 10, 'rainfall_7d': np.random.rand() * 10,
                'rainfall_1m': np.random.rand() * 20, 'permanent_water': permanent_water_val,
                'water_presence': water_presence_val, 'water_cluster': water_cluster_val})
    df_dummy = pd.DataFrame(data)
    df_dummy['water_cluster'] = df_dummy['water_cluster'].astype(float)
    return df_dummy

def print_tensor_info(tensor, name="Tensor"):
    """
    Print shape, dtype, min/max/mean of a tensor.
    """
    print(f"{name}: shape={tuple(tensor.shape)}, dtype={tensor.dtype}, min={tensor.min().item():.4f}, max={tensor.max().item():.4f}, mean={tensor.mean().item():.4f}")

def set_seed(seed=42):
    """
    Set seed for numpy, torch, random to ensure reproducibility.
    """
    np.random.seed(seed)
    torch.manual_seed(seed)
    random.seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
    os.environ["PYTHONHASHSEED"] = str(seed)

@asset
def utils():
    """
    Dagster Asset: Utility functions for the pipeline.
    """
    return {
        "parse_list_string_to_2d_array": parse_list_string_to_2d_array,
        "create_dummy_data": create_dummy_data,
        "print_tensor_info": print_tensor_info,
        "set_seed": set_seed
    }