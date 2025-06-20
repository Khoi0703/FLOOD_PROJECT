import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, GATConv
from dagster import asset

class CNNEncoder(nn.Module):
    """
    2D CNN Encoder for extracting spatial features from height maps.
    """
    def __init__(self, input_channels, filters, kernel_sizes, strides, paddings, cnn_output_dim, img_h, img_w, dropout_rate):
        super(CNNEncoder, self).__init__()
        self.conv_layers = nn.ModuleList()
        current_channels = input_channels
        current_h, current_w = img_h, img_w

        for i in range(len(filters)):
            self.conv_layers.append(
                nn.Conv2d(current_channels, filters[i], kernel_sizes[i], strides[i], paddings[i])
            )
            self.conv_layers.append(nn.ReLU())
            self.conv_layers.append(nn.MaxPool2d(kernel_size=2, stride=2))
            self.conv_layers.append(nn.Dropout2d(dropout_rate))
            current_channels = filters[i]
            current_h = (current_h - 2 + 2 * paddings[i]) // strides[i] + 1
            current_h = (current_h - 2) // 2 + 1
            current_w = (current_w - 2 + 2 * paddings[i]) // strides[i] + 1
            current_w = (current_w - 2) // 2 + 1

        self.flatten = nn.Flatten()
        # Dynamically calculate flattened size
        with torch.no_grad():
            dummy_input = torch.zeros(1, input_channels, img_h, img_w)
            for layer in self.conv_layers:
                dummy_input = layer(dummy_input)
            flattened_size = dummy_input.shape[1] * dummy_input.shape[2] * dummy_input.shape[3]

        self.fc = nn.Linear(flattened_size, cnn_output_dim)
        self.dropout_fc = nn.Dropout(dropout_rate)

    def forward(self, x):
        for layer in self.conv_layers:
            x = layer(x)
        x = self.flatten(x)
        x = self.dropout_fc(x)
        x = F.relu(self.fc(x))
        return x

class GNNModel(nn.Module):
    """
    GNN model for learning node relationships.
    Supports GCN and GAT layers.
    """
    def __init__(self, node_feature_dim, gnn_hidden_dim, output_dim, gnn_layers, gnn_type='GCN', dropout=0.3, attention_heads=4):
        super(GNNModel, self).__init__()
        self.gnn_type = gnn_type
        self.convs = nn.ModuleList()
        input_channels = node_feature_dim
        for i in range(gnn_layers):
            output_channels = gnn_hidden_dim
            if gnn_type == 'GCN':
                self.convs.append(GCNConv(input_channels, output_channels))
            elif gnn_type == 'GAT':
                if i == gnn_layers - 1:
                    self.convs.append(GATConv(input_channels, output_channels, heads=1, concat=False, dropout=dropout))
                else:
                    self.convs.append(GATConv(input_channels, output_channels // attention_heads, heads=attention_heads, dropout=dropout))
                    input_channels = output_channels
            else:
                raise ValueError(f"Unsupported GNN type: {gnn_type}")
            if gnn_type != 'GAT' or i == gnn_layers - 1:
                input_channels = output_channels
        self.dropout_layer = nn.Dropout(dropout)
        self.out_layer = nn.Linear(gnn_hidden_dim, output_dim)

    def forward(self, x_node, edge_index):
        for i, conv in enumerate(self.convs):
            x_node = conv(x_node, edge_index)
            if not (self.gnn_type == 'GAT' and i == len(self.convs) -1 and hasattr(conv, 'concat') and not conv.concat):
                x_node = F.relu(x_node)
            if i < len(self.convs) - 1:
                x_node = self.dropout_layer(x_node)
        output = self.out_layer(x_node)
        return output

class CombinedModel(nn.Module):
    """
    Combines CNNEncoder and GNNModel for end-to-end flood prediction.
    """
    def __init__(self, cnn_input_channels, cnn_filters, cnn_kernel_sizes, cnn_strides, cnn_paddings,
                 cnn_output_dim, img_h, img_w, cnn_dropout,
                 num_scalar_features, gnn_type, gnn_hidden_dim, gnn_output_dim, gnn_layers, gnn_dropout, gnn_attention_heads):
        super(CombinedModel, self).__init__()
        self.cnn_encoder = CNNEncoder(cnn_input_channels, cnn_filters, cnn_kernel_sizes, cnn_strides, cnn_paddings,
                                      cnn_output_dim, img_h, img_w, cnn_dropout)
        gnn_input_node_dim = cnn_output_dim + num_scalar_features
        self.gnn = GNNModel(gnn_input_node_dim, gnn_hidden_dim, gnn_output_dim, gnn_layers, gnn_type, gnn_dropout, gnn_attention_heads)

    def forward(self, spatial_data, scalar_data, edge_index):
        cnn_features = self.cnn_encoder(spatial_data)
        combined_node_features = torch.cat([cnn_features, scalar_data], dim=1)
        output = self.gnn(combined_node_features, edge_index)
        return output

def train_epoch(model, optimizer, criterion, data_obj, device):
    model.train()
    optimizer.zero_grad()
    spatial_data = data_obj.x_spatial.to(device)
    scalar_data = data_obj.x_scalar.to(device)
    edge_idx = data_obj.edge_index.to(device)
    targets = data_obj.y.to(device)
    current_train_mask = data_obj.train_mask.to(device)

    out = model(spatial_data, scalar_data, edge_idx)
    loss = criterion(out[current_train_mask], targets[current_train_mask])
    loss.backward()
    optimizer.step()
    return loss.item()

@torch.no_grad()
def evaluate_epoch(model, criterion, data_obj, mask, device):
    model.eval()
    spatial_data = data_obj.x_spatial.to(device)
    scalar_data = data_obj.x_scalar.to(device)
    edge_idx = data_obj.edge_index.to(device)
    targets = data_obj.y.to(device)
    current_eval_mask = mask.to(device)

    out = model(spatial_data, scalar_data, edge_idx)
    loss = criterion(out[current_eval_mask], targets[current_eval_mask])
    mae = F.l1_loss(out[current_eval_mask], targets[current_eval_mask])
    return loss.item(), mae.item(), out

@asset
def model(preprocessing) -> CombinedModel:
    """
    Asset Dagster: Khởi tạo model với tham số giống notebook Flood_Prediction_with_2D_CNN_and_GNN.ipynb.
    """
    num_scalar_features = preprocessing["scalar_features_tensor"].shape[1]
    img_h, img_w = 40, 40  # IMG_HEIGHT, IMG_WIDTH trong notebook

    # Tham số CNN giống notebook
    cnn_input_channels = 1  # NUM_IMG_CHANNELS
    cnn_filters = [16, 32, 64]  # CNN_FILTERS
    cnn_kernel_sizes = [3, 3, 3]  # CNN_KERNEL_SIZES
    cnn_strides = [1, 1, 1]  # CNN_STRIDES
    cnn_paddings = [1, 1, 1]  # CNN_PADDINGS
    cnn_output_dim = 128  # CNN_OUTPUT_DIM
    cnn_dropout = 0.25  # CNN_DROPOUT

    # Tham số GNN giống notebook
    gnn_type = "GCN"  # GNN_TYPE
    gnn_hidden_dim = 128  # GNN_HIDDEN_DIM
    gnn_output_dim = preprocessing["target_flood_tensor"].shape[1]  # OUTPUT_DIM = IMG_HEIGHT * IMG_WIDTH
    gnn_layers = 2  # GNN_LAYERS
    gnn_dropout = 0.3  # GNN_DROPOUT
    gnn_attention_heads = 4  # GNN_ATTENTION_HEADS

    return CombinedModel(
        cnn_input_channels, cnn_filters, cnn_kernel_sizes, cnn_strides, cnn_paddings,
        cnn_output_dim, img_h, img_w, cnn_dropout,
        num_scalar_features, gnn_type, gnn_hidden_dim, gnn_output_dim, gnn_layers, gnn_dropout, gnn_attention_heads
    )
