from dagster import asset
import torch

@torch.no_grad()
def evaluate_model_on_test(model, data_obj, device, verbose=True, num_show=5):
    """
    Evaluate the model on the test set, returning loss, MAE, and printing example predictions/actual values.
    """
    model.eval()
    spatial_data = data_obj["x_spatial"].to(device)
    scalar_data = data_obj["x_scalar"].to(device)
    edge_idx = data_obj["edge_index"].to(device)
    targets = data_obj["y"].to(device)
    test_mask = data_obj["test_mask"].to(device)

    out = model(spatial_data, scalar_data, edge_idx)
    mse_loss = torch.nn.functional.mse_loss(out[test_mask], targets[test_mask]).item()
    mae = torch.nn.functional.l1_loss(out[test_mask], targets[test_mask]).item()

    if verbose:
        print(f'Test Set Results: MSE Loss: {mse_loss:.4f}, MAE: {mae:.4f}')
        # Display example prediction/actual value for 1 test node
        original_test_node_indices = torch.where(test_mask)[0]
        if len(original_test_node_indices) > 0:
            example_node_idx_in_test_set = 0
            original_node_idx = original_test_node_indices[example_node_idx_in_test_set].item()
            predicted_values = out[original_node_idx, :num_show].detach().cpu().numpy()
            actual_values = targets[original_node_idx, :num_show].detach().cpu().numpy()
            print(f"\nExample - Test Node (Original Index: {original_node_idx}), first {num_show} (flattened) flood values:")
            print(f"Predicted: {predicted_values}")
            print(f"Actual:    {actual_values}")
        else:
            print("No nodes in the test set to display an example.")

    return mse_loss, mae, out

@asset
def evaluation(training) -> dict:
    """
    Dagster Asset: Evaluate the model on the test set.
    """
    model = training["model"]
    data_obj = training["data_obj"]
    device = training["device"]
    mse_loss, mae, out = evaluate_model_on_test(model, data_obj, device, verbose=True)
    # Save the model
    model_path = "data/final/best_cnn_gnn_model.pth"
    torch.save(model.state_dict(), model_path)
    print(f"Model saved to {model_path}")

    # Kiểm tra phân phối ground truth trên test set
    test_mask = data_obj["test_mask"]
    targets = data_obj["y"]
    test_targets = targets[test_mask]
    num_nodes = test_targets.shape[0]
    num_nonzero = (test_targets != 0).sum().item()
    percent_nonzero = 100 * num_nonzero / test_targets.numel()
    print(f"Test set: {num_nodes} nodes, {percent_nonzero:.2f}% of flood values are nonzero.")

    # Lấy ra tất cả node test có ground truth khác 0
    nonzero_indices = (test_targets.abs().sum(dim=1) > 0).nonzero(as_tuple=True)[0]
    nonzero_pred = None
    nonzero_actual = None
    if len(nonzero_indices) > 0:
        # Lấy toàn bộ giá trị dự đoán và thực tế của các node này
        nonzero_pred = out[test_mask][nonzero_indices].detach().cpu().numpy()
        nonzero_actual = test_targets[nonzero_indices].detach().cpu().numpy()
        print(f"\nCó {len(nonzero_indices)} test node có ground truth khác 0.")
        for i in range(len(nonzero_indices)):
            print(f"Node {i}:")
            print("  Predicted:", nonzero_pred[i][:5])
            print("  Actual:   ", nonzero_actual[i][:5])
    else:
        print("No test node has nonzero ground truth flood values.")

    return {
        "mse_loss": mse_loss,
        "mae": mae,
        "predictions": out,
        "model_path": model_path,
        "nonzero_pred": nonzero_pred,
        "nonzero_actual": nonzero_actual
    }