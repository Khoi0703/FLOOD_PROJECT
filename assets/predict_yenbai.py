from dagster import asset
import pandas as pd
import torch
import numpy as np
from assets.preprocessing import parse_list_string_to_2d_array
from sklearn.preprocessing import StandardScaler

@asset
def predict_yenbai(training, yenbai_rain: pd.DataFrame) -> pd.DataFrame:
    model = training["model"]
    device = training["device"]
    model.eval()

    # === Load dữ liệu mới để predict ===
    df = pd.read_csv("data/final/yenbai_rainfall.csv")

    # Backup bản gốc chưa chuẩn hóa để xuất CSV
    df_original = df.copy()

    # Nếu thiếu flood_values, tạo dummy
    if "flood_values" not in df.columns:
        df["flood_values"] = [[0.0] * 1600] * len(df)

    # Parse height_values & flood_values về 2D 40x40
    df["height_values"] = df["height_values"].apply(lambda s: parse_list_string_to_2d_array(s, 40, 40))
    df["flood_values"] = df["flood_values"].apply(lambda s: parse_list_string_to_2d_array(s, 40, 40))

    # === Dùng đúng scalar features và scaler như lúc training ===
    required_features = training.get("scalar_features", [
        "square_center_lat", "square_center_lon",
        "rainfall_3d", "rainfall_7d", "rainfall_1m",
        "permanent_water", "water_presence"
    ])  # Bắt buộc phải có, không fallback mặc định!
    missing = [col for col in required_features if col not in df.columns]
    if missing:
        print(f"[WARNING] Các cột sau bị thiếu trong yenbai_final.csv, sẽ điền 0: {missing}")
        for col in missing:
            df[col] = 0.0

    # Dùng lại scaler đã fit từ training, không fit lại!
    scaler = training["scaler"]
    df_scaled = scaler.transform(df[required_features])
    scalar_tensor = torch.tensor(df_scaled, dtype=torch.float32)

    # Xử lý height_values
    spatial_np = torch.tensor([h for h in df["height_values"].values], dtype=torch.float32).unsqueeze(1)

    # Dummy edge_index nếu không có kết nối
    num_nodes = len(df)
    edge_index = torch.empty((2, 0), dtype=torch.long)

    # === Predict ===
    with torch.no_grad():
        pred = model(spatial_np.to(device), scalar_tensor.to(device), edge_index.to(device))
        pred_np = pred.cpu().numpy().reshape(-1, 40, 40)
        df["pred_flood_values"] = [pred_np[i].tolist() for i in range(num_nodes)]

    # === Tính tổng độ ngập (mean các giá trị > 0) ===
    def mean_nonzero(arr):
        arr = np.array(arr)
        arr = arr[arr > 0]
        return arr.mean() if arr.size > 0 else 0.0

    df["pred_flood_score"] = df["pred_flood_values"].apply(mean_nonzero)

    # === In Top 10 vùng có độ ngập cao nhất ===
    top10 = df_original.copy()
    top10["pred_flood_score"] = df["pred_flood_score"]
    cols = ["square_center_lat", "square_center_lon", "pred_flood_score"]
    if "big_square_id" in top10.columns:
        cols.insert(0, "big_square_id")

    top10_display = top10[cols].sort_values(by="pred_flood_score", ascending=False).head(10)

    print("🌊 Top 10 khu vực dự đoán có độ ngập cao nhất:")
    print(top10_display)

    # === Xuất CSV sạch, giữ lại dữ liệu gốc chưa chuẩn hóa ===
    output_cols = [
        "big_square_id" if "big_square_id" in df_original.columns else None,
        "square_center_lat", "square_center_lon",
        "rainfall_3d", "rainfall_7d", "rainfall_1m",
        "permanent_water", "water_presence",
        "pred_flood_score"
    ]
    output_cols = [col for col in output_cols if col is not None]
    df_output = df_original.copy()
    df_output["pred_flood_score"] = df["pred_flood_score"]
    df_output.to_csv("data/final/yenbai_predictions_clean.csv", index=False)

    return df
