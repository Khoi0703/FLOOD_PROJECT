from dagster import asset
import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
from tqdm import tqdm

# --- Simple Training Loop (not Dagster asset) ---
def simple_train_loop(model, graph_data, device, num_epochs=50, patience_epochs=10, lr=0.001, weight_decay=5e-4):
    optimizer = optim.Adam(model.parameters(), lr=lr, weight_decay=weight_decay)
    criterion = nn.MSELoss()
    best_val_loss = float('inf')
    patience_counter = 0

    def train_epoch():
        model.train()
        optimizer.zero_grad()
        # Use dict access instead of attribute access
        spatial_data = graph_data["x_spatial"].to(device)
        scalar_data = graph_data["x_scalar"].to(device)
        edge_idx = graph_data["edge_index"].to(device)
        targets = graph_data["y"].to(device)
        current_train_mask = graph_data["train_mask"].to(device)

        print(f"[DEBUG] spatial_data shape: {spatial_data.shape}")
        print(f"[DEBUG] scalar_data shape: {scalar_data.shape}")
        assert spatial_data.shape[0] == scalar_data.shape[0], (
            f"Shape mismatch: spatial_data.shape[0]={spatial_data.shape[0]} vs scalar_data.shape[0]={scalar_data.shape[0]}"
        )

        # --- Check for NaN/Inf in inputs ---
        def check_nan_inf(name, tensor):
            if torch.isnan(tensor).any():
                print(f"[ERROR] {name} contains NaN!")
                print(f"NaN indices: {torch.isnan(tensor).nonzero()}")
                raise ValueError(f"{name} contains NaN")
            if torch.isinf(tensor).any():
                print(f"[ERROR] {name} contains Inf!")
                print(f"Inf indices: {torch.isinf(tensor).nonzero()}")
                raise ValueError(f"{name} contains Inf")

        check_nan_inf("spatial_data", spatial_data)
        check_nan_inf("scalar_data", scalar_data)
        check_nan_inf("targets", targets)

        out = model(spatial_data, scalar_data, edge_idx)
        print(f"[DEBUG] model output shape: {out.shape}")
        print(f"[DEBUG] targets shape: {targets.shape}")

        # --- Check for NaN/Inf in model output ---
        check_nan_inf("model_output", out)

        loss = criterion(out[current_train_mask], targets[current_train_mask])
        # --- Check for NaN/Inf in loss ---
        if torch.isnan(loss) or torch.isinf(loss):
            print(f"[ERROR] Loss is NaN or Inf! Stopping training.")
            raise ValueError("Loss is NaN or Inf")
        loss.backward()
        optimizer.step()
        return loss.item()

    @torch.no_grad()
    def evaluate_epoch(mask):
        model.eval()
        spatial_data = graph_data["x_spatial"].to(device)
        scalar_data = graph_data["x_scalar"].to(device)
        edge_idx = graph_data["edge_index"].to(device)
        targets = graph_data["y"].to(device)
        current_eval_mask = mask.to(device)

        out = model(spatial_data, scalar_data, edge_idx)
        loss = criterion(out[current_eval_mask], targets[current_eval_mask])
        mae = F.l1_loss(out[current_eval_mask], targets[current_eval_mask])
        return loss.item(), mae.item(), out

    print(f"\nStarting training for {num_epochs} epochs...")
    for epoch in tqdm(range(1, num_epochs + 1)):
        train_loss = train_epoch()
        val_loss, val_mae, _ = evaluate_epoch(graph_data["val_mask"])

        if epoch % (num_epochs//10 if num_epochs >=10 else 1) == 0 or epoch == 1:
            print(f'Epoch: {epoch:03d}, Train Loss: {train_loss:.4f}, Val Loss: {val_loss:.4f}, Val MAE: {val_mae:.4f}')

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            patience_counter = 0
            # torch.save(model.state_dict(), 'best_cnn_gnn_model.pth')
        else:
            patience_counter += 1
            if patience_counter >= patience_epochs:
                print(f"Early stopping at epoch {epoch} due to no improvement in val_loss for {patience_epochs} epochs.")
                break

    return model

@asset
def training(model, preprocessing) -> dict:
    """
    Asset Dagster: Train model với dữ liệu đã preprocessing.
    """
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)

    # Debug: print available keys in preprocessing
    print("[DEBUG] preprocessing keys:", list(preprocessing.keys()))

    # Lấy scalar features và scaler từ preprocessing (giả sử preprocessing đã trả về)
    scalar_features = preprocessing.get("scalar_features", None)
    scaler = preprocessing.get("scaler", None)
    if scalar_features is None or scaler is None:
        raise KeyError(
            f"Missing keys in preprocessing: "
            f"{'scalar_features' if scalar_features is None else ''} "
            f"{'scaler' if scaler is None else ''}. "
            f"Available keys: {list(preprocessing.keys())}. "
            "Check the output of the preprocessing asset."
        )

    # Chuẩn bị data object (dùng dict thay vì class để tránh lỗi pickle)
    data_obj = {
        "x_spatial": preprocessing["height_spatial_tensor"],
        "x_scalar": preprocessing["scalar_features_tensor"],
        "edge_index": preprocessing["edge_index"],
        "y": preprocessing["target_flood_tensor"],
        "train_mask": preprocessing["train_mask"],
        "val_mask": preprocessing["val_mask"],
        "test_mask": preprocessing["test_mask"],
    }

    # --- Add shape assertion for debugging ---
    num_nodes = data_obj["x_spatial"].shape[0]
    assert data_obj["train_mask"].shape[0] == num_nodes, (
        f"train_mask shape {data_obj['train_mask'].shape} does not match number of nodes {num_nodes}. "
        "Check mask creation in preprocessing."
    )
    assert data_obj["y"].shape[0] == num_nodes, (
        f"Target tensor shape {data_obj['y'].shape} does not match number of nodes {num_nodes}."
    )
    # -----------------------------------------

    # Sử dụng simple_train_loop thay cho train_model
    simple_train_loop(
        model, data_obj, device,
        num_epochs=50, patience_epochs=10, lr=0.001, weight_decay=5e-4
    )
    result = {
        "model": model,
        "scaler": scaler,  # scaler đã fit
        "scalar_features": scalar_features,  # danh sách tên cột
        "device": device,
        "data_obj": data_obj,  # <-- ensure this is present and is the processed tensor dict
    }
    return result