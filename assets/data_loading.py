from dagster import asset
import pandas as pd
import numpy as np
import torch
from sklearn.preprocessing import StandardScaler
import ast
import itertools

def parse_list_string_to_2d_array(s, img_height, img_width):
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

@asset
def data_loading(context, water_cluster: pd.DataFrame = None) -> dict:
    """
    Asset Dagster: Tiền xử lý dữ liệu từ water_cluster DataFrame hoặc tạo dummy nếu không có.
    """
    img_height, img_width = 40, 40
    fill_water_cluster = -1
    scalar_features_base = [
        'square_center_lat', 'square_center_lon',
        'rainfall_3d', 'rainfall_7d', 'rainfall_1m',
        'permanent_water', 'water_presence'
    ]

    # Nếu không có csv hoặc water_cluster là None hoặc rỗng, tạo dummy data
    if water_cluster is None or len(water_cluster) == 0:
        context.log.warning("No input DataFrame provided. Using dummy data.")
        df = create_dummy_data(img_height, img_width, num_events=5, squares_per_event_range=(10, 20), num_water_clusters=8)
        print("DataFrame head (dummy):")
        print(df.head())
        print(f"\nDataFrame shape: {df.shape}")
    else:
        df = water_cluster.copy()
        print("DataFrame head (loaded):")
        print(df.head())
        print(f"\nDataFrame shape: {df.shape}")

    # Parse list strings to 2D arrays
    df['height_values'] = df['height_values'].apply(lambda s: parse_list_string_to_2d_array(s, img_height, img_width))
    df['flood_values'] = df['flood_values'].apply(lambda s: parse_list_string_to_2d_array(s, img_height, img_width))

    # Feature engineering: event_duration_days
    if 'began_date' in df.columns and 'ended_date' in df.columns:
        try:
            df['began_date'] = pd.to_datetime(df['began_date'])
            df['ended_date'] = pd.to_datetime(df['ended_date'])
            df['event_duration_days'] = (df['ended_date'] - df['began_date']).dt.days
            df['event_duration_days'] = df['event_duration_days'].fillna(0).astype(float)
        except Exception:
            df['event_duration_days'] = 0.0

    # Handle missing values
    df['water_presence'] = df['water_presence'].fillna(0.0)
    if 'water_cluster' in df.columns:
        df['water_cluster'] = df['water_cluster'].fillna(fill_water_cluster).astype(int)
    else:
        df['water_cluster'] = fill_water_cluster

    # Scalar features
    if 'event_duration_days' in df.columns:
        scalar_features = scalar_features_base + ['event_duration_days']
    else:
        scalar_features = scalar_features_base
    available_scalar_features = [col for col in scalar_features if col in df.columns]
    scaler = StandardScaler()
    if available_scalar_features:
        df[available_scalar_features] = scaler.fit_transform(df[available_scalar_features])
        scalar_features_tensor = torch.tensor(df[available_scalar_features].values, dtype=torch.float32)
    else:
        scalar_features_tensor = torch.empty((len(df), 0), dtype=torch.float32)

    # Node index mapping
    if 'event_square_id' not in df.columns:
        df['event_square_id'] = [f'dummy_{i}' for i in range(len(df))]
    unique_ids = df['event_square_id'].unique()
    id_to_node_idx = {uid: i for i, uid in enumerate(unique_ids)}
    df['node_idx'] = df['event_square_id'].map(id_to_node_idx)

    # Prepare tensors
    height_spatial_data = np.stack(df['height_values'].values)
    height_spatial_tensor = torch.tensor(height_spatial_data, dtype=torch.float32).unsqueeze(1)
    target_flood_flat = np.stack(df['flood_values'].values).reshape(len(df), -1)
    target_flood_tensor = torch.tensor(target_flood_flat, dtype=torch.float32)

    # Build edge_index
    edge_list = []
    if 'event_index' in df.columns:
        for _, group in df.groupby('event_index'):
            for u, v in itertools.combinations(group['node_idx'].tolist(), 2):
                edge_list.extend([(u, v), (v, u)])
    if 'water_cluster' in df.columns:
        for cluster_idx, group in df.groupby('water_cluster'):
            if cluster_idx != fill_water_cluster:
                for u, v in itertools.combinations(group['node_idx'].tolist(), 2):
                    edge_list.extend([(u, v), (v, u)])
    if edge_list:
        edge_index = torch.tensor(list(set(edge_list)), dtype=torch.long).t().contiguous()
    else:
        edge_index = torch.empty((2, 0), dtype=torch.long)

    context.log.info(f"✅ Data loaded: {len(df)} samples, {len(available_scalar_features)} scalar features.")
    context.log.info(f"DataFrame shape after loading: {df.shape}")

    return {
        "df": df,
        "scalar_features_tensor": scalar_features_tensor,
        "height_spatial_tensor": height_spatial_tensor,
        "target_flood_tensor": target_flood_tensor,
        "edge_index": edge_index,
        "id_to_node_idx": id_to_node_idx,
        "available_scalar_features": available_scalar_features
    }

def create_dummy_data(img_height, img_width, num_events=3, squares_per_event_range=(5, 10), num_water_clusters=5):
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