from dagster import asset
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN

@asset
def run_dbscan_clustering(context) -> pd.DataFrame:
    input_path = "data/raw/flood_data.csv"
    df = pd.read_csv(input_path)

    coords = df[['dfo_centroid_y', 'dfo_centroid_x']].values
    coords_rad = np.radians(coords)

    kms_per_radian = 6371.0088
    epsilon = 50 / kms_per_radian  # 50km

    db = DBSCAN(eps=epsilon, min_samples=2, algorithm='ball_tree', metric='haversine')
    db.fit(coords_rad)
    df['cluster'] = db.labels_

    df['event_index'] = df.index
    df = df[df['cluster'] != -1].copy().reset_index(drop=True)

    output_path = "data/intermediate/flood_events_nearby_clusters.csv"
    df.to_csv(output_path, index=False)
    context.log.info(f"✅ Saved clustered events to {output_path}")

    return df
