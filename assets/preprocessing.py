from dagster import asset
import numpy as np
import torch
from sklearn.preprocessing import StandardScaler
import itertools
import pandas as pd
import ast
from torch_geometric.data import Data

IMG_HEIGHT = 40
IMG_WIDTH = 40

def parse_list_string_to_2d_array(s, IMG_HEIGHT=IMG_HEIGHT, IMG_WIDTH=IMG_WIDTH):
    flat_list_len = IMG_HEIGHT * IMG_WIDTH
    if isinstance(s, (list, np.ndarray)):
        arr = np.array(s, dtype=np.float32)
        if arr.ndim == 2 and arr.shape == (IMG_HEIGHT, IMG_WIDTH): return arr
        elif arr.ndim == 1 and len(arr) == flat_list_len: return arr.reshape(IMG_HEIGHT, IMG_WIDTH)
        else: return np.zeros((IMG_HEIGHT, IMG_WIDTH), dtype=np.float32)
    if not isinstance(s, str): return np.zeros((IMG_HEIGHT, IMG_WIDTH), dtype=np.float32)
    try:
        arr = np.array(ast.literal_eval(s), dtype=np.float32)
        if len(arr) == flat_list_len: return arr.reshape(IMG_HEIGHT, IMG_WIDTH)
        else: return np.zeros((IMG_HEIGHT, IMG_WIDTH), dtype=np.float32)
    except (ValueError, SyntaxError): return np.zeros((IMG_HEIGHT, IMG_WIDTH), dtype=np.float32)

def preprocess_flood_dataframe(
    df,
    img_height=IMG_HEIGHT,
    img_width=IMG_WIDTH,
    fill_water_cluster=-1,
    train_ratio=0.7,
    val_ratio=0.15,
    random_seed=42
):
    # --- Parse height_values and flood_values columns ---
    print("Parsing height_values and flood_values columns...")
    df['height_values'] = df['height_values'].apply(lambda s: parse_list_string_to_2d_array(s, img_height, img_width))
    df['flood_values'] = df['flood_values'].apply(lambda s: parse_list_string_to_2d_array(s, img_height, img_width))
    print(f"height_values shape example: {df['height_values'].iloc[0].shape}")
    print(f"flood_values shape example: {df['flood_values'].iloc[0].shape}")

    # --- Feature Engineering ---
    # Event duration
    print("Calculating event_duration_days...")
    if 'began_date' in df.columns and 'ended_date' in df.columns:
        try:
            df['began_date'] = pd.to_datetime(df['began_date'])
            df['ended_date'] = pd.to_datetime(df['ended_date'])
            df['event_duration_days'] = (df['ended_date'] - df['began_date']).dt.days
            df['event_duration_days'] = df['event_duration_days'].fillna(0).astype(float)
        except Exception as e:
            print(f"Warning: Could not process dates for event_duration_days. Error: {e}. Creating with zeros.")
            df['event_duration_days'] = 0.0

    # Handle missing values
    print("Filling missing values for water_presence and water_cluster...")
    df['water_presence'] = df['water_presence'].fillna(0.0)
    if 'water_cluster' in df.columns:
        df['water_cluster'] = df['water_cluster'].fillna(fill_water_cluster).astype(int)
    else:
        print("Warning: 'water_cluster' column not found. Creating it with default value -1.")
        df['water_cluster'] = fill_water_cluster

    # Scalar features
    scalar_features_base = [
        'square_center_lat', 'square_center_lon',
        'rainfall_3d', 'rainfall_7d', 'rainfall_1m',
        'permanent_water', 'water_presence'
    ]
    if 'event_duration_days' in df.columns:
        scalar_features_base.append('event_duration_days')
    available_scalar_features = [col for col in scalar_features_base if col in df.columns]
    print(f"Available scalar features: {available_scalar_features}")
    if not available_scalar_features:
        print("Warning: No scalar features found for scaling.")
        scalar_features_tensor = torch.empty((len(df), 0), dtype=torch.float32)
        num_scalar_features = 0
    else:
        # --- Check for NaN before scaling ---
        nan_rows = df[available_scalar_features].isna().any(axis=1)
        if nan_rows.any():
            print("[WARNING] Rows with NaN in scalar features before scaling:")
            print(df.loc[nan_rows, available_scalar_features])
        scaler = StandardScaler()
        df[available_scalar_features] = scaler.fit_transform(df[available_scalar_features])
        # --- Replace any remaining NaN with 0 after scaling ---
        df[available_scalar_features] = df[available_scalar_features].fillna(0.0)
        # --- Check for NaN after scaling ---
        nan_rows_after = df[available_scalar_features].isna().any(axis=1)
        if nan_rows_after.any():
            print("[WARNING] Rows with NaN in scalar features after scaling (should not happen):")
            print(df.loc[nan_rows_after, available_scalar_features])
        scalar_features_tensor = torch.tensor(df[available_scalar_features].values, dtype=torch.float32)
        num_scalar_features = scalar_features_tensor.shape[1]
        print(f"Using {num_scalar_features} scalar features: {available_scalar_features}")
        print(f"Scalar features tensor shape: {scalar_features_tensor.shape}")

    # Node index mapping
    if 'event_square_id' not in df.columns:
        df['event_square_id'] = [f'dummy_{i}' for i in range(len(df))]
    unique_ids = pd.unique(df['event_square_id'])
    id_to_node_idx = {uid: i for i, uid in enumerate(unique_ids)}
    df['node_idx'] = df['event_square_id'].map(id_to_node_idx)
    print(f"Number of unique nodes: {len(unique_ids)}")

    # Ensure one row per node (drop duplicates, keep first)
    df = df.drop_duplicates(subset=['event_square_id'])
    # Sort df to match unique_ids order
    df = df.set_index('event_square_id').loc[unique_ids].reset_index()

    # --- FIX: Only use deduplicated/sorted df for all tensors ---
    # Prepare tensors
    print("Stacking height_values and flood_values to tensors...")
    height_spatial_data = np.stack(df['height_values'].values)
    height_spatial_tensor = torch.tensor(height_spatial_data, dtype=torch.float32).unsqueeze(1)
    print(f"height_spatial_tensor shape: {height_spatial_tensor.shape}")
    target_flood_flat = np.stack(df['flood_values'].values).reshape(len(df), -1)
    target_flood_tensor = torch.tensor(target_flood_flat, dtype=torch.float32)
    print(f"target_flood_tensor shape: {target_flood_tensor.shape}")

    # --- FIX: Scalar features tensor must also use deduplicated/sorted df ---
    if not available_scalar_features:
        scalar_features_tensor = torch.empty((len(df), 0), dtype=torch.float32)
        num_scalar_features = 0
    else:
        scalar_features_tensor = torch.tensor(df[available_scalar_features].values, dtype=torch.float32)
        num_scalar_features = scalar_features_tensor.shape[1]
        print(f"Scalar features tensor shape: {scalar_features_tensor.shape}")

    # Build edge_index
    print("Building edge_index...")
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
        print("Warning: No edges created.")
        edge_index = torch.empty((2, 0), dtype=torch.long)
    print(f"Number of edges: {edge_index.shape[1]}")

    # --- Data Splitting (Node-level Masks) ---
    print("Splitting data into train/val/test masks...")
    num_nodes = len(unique_ids)
    node_indices = np.arange(num_nodes)
    rng = np.random.default_rng(random_seed)
    rng.shuffle(node_indices)
    train_end = int(train_ratio * num_nodes)
    val_end = train_end + int(val_ratio * num_nodes)
    train_nodes = torch.tensor(node_indices[:train_end], dtype=torch.long)
    val_nodes = torch.tensor(node_indices[train_end:val_end], dtype=torch.long)
    test_nodes = torch.tensor(node_indices[val_end:], dtype=torch.long)
    train_mask = torch.zeros(num_nodes, dtype=torch.bool)
    val_mask = torch.zeros(num_nodes, dtype=torch.bool)
    test_mask = torch.zeros(num_nodes, dtype=torch.bool)
    train_mask[train_nodes] = True
    val_mask[val_nodes] = True
    test_mask[test_nodes] = True
    print(f"Train nodes: {train_mask.sum().item()}, Val nodes: {val_mask.sum().item()}, Test nodes: {test_mask.sum().item()}")

    # Create graph data object
    graph_data = Data(
        x_scalar=scalar_features_tensor,
        x_spatial=height_spatial_tensor,  # Renamed from x_sequence
        edge_index=edge_index,
        y=target_flood_tensor,  # Target is now flattened 2D flood map
        train_mask=train_mask,
        val_mask=val_mask,
        test_mask=test_mask
    )
    print("\nGraph Data object:")
    print(graph_data)
    print("x_scalar shape:", graph_data.x_scalar.shape)
    print("x_spatial shape:", graph_data.x_spatial.shape)
    print("edge_index shape:", graph_data.edge_index.shape)
    print("y shape:", graph_data.y.shape)
    print("train_mask sum:", graph_data.train_mask.sum().item())
    print("val_mask sum:", graph_data.val_mask.sum().item())
    print("test_mask sum:", graph_data.test_mask.sum().item())

    # --- Return preprocessed data as a dictionary ---
    return {
        "df": df,
        "scalar_features_tensor": scalar_features_tensor,
        "height_spatial_tensor": height_spatial_tensor,
        "target_flood_tensor": target_flood_tensor,
        "edge_index": edge_index,
        "id_to_node_idx": id_to_node_idx,
        "available_scalar_features": available_scalar_features,
        "scalar_features": available_scalar_features,
        "scaler": scaler if 'scaler' in locals() else None,
        "train_mask": train_mask,
        "val_mask": val_mask,
        "test_mask": test_mask,
        "num_scalar_features": num_scalar_features
    }

@asset
def preprocessing(data_loading: dict = None, utils: dict = None) -> dict:
    """
    Asset Dagster: Tiền xử lý dữ liệu đầu vào từ asset data_loading hoặc tạo dữ liệu giả nếu không có.
    """
    if data_loading is not None:
        df = data_loading["df"]
    elif utils is not None and "create_dummy_data" in utils:
        # Nếu không có data_loading thì dùng utils để tạo dummy data
        df = utils["create_dummy_data"](img_height=40, img_width=40)
    else:
        raise ValueError("Cần có data_loading hoặc utils để tạo dữ liệu đầu vào.")
    return preprocess_flood_dataframe(df)

