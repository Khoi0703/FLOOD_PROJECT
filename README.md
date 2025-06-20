Dưới đây là phiên bản đầy đủ và **chuẩn hóa chuyên nghiệp** của file `README.md` cho dự án `flood_project`, được cập nhật dựa trên **cấu trúc cây bạn cung cấp**:

---

````markdown
# 🌊 Flood Prediction Project - Yên Bái, Vietnam

Dự án dự báo và cảnh báo ngập lụt tại tỉnh Yên Bái dựa trên dữ liệu khí tượng, địa hình, và thủy văn. Hệ thống bao gồm:

- ✅ Pipeline Dagster: xử lý, gom cụm, huấn luyện và dự đoán.
- 🧠 Mô hình học sâu GNN-CNN-LSTM.
- 🌐 Ứng dụng Streamlit trực quan hóa dữ liệu mưa toàn quốc và cảnh báo ngập ở Yên Bái.

---

## 📂 Cấu trúc thư mục

```bash
flood_project/
│
├── README.md                  # Tài liệu này
├── requirements.txt           # Thư viện Python cần thiết
│
└── flood_pipeline/
    ├── flood_pipeline.py      # Entry point pipeline Dagster
    ├── workspace.yaml         # Cấu hình workspace Dagster
    │
    ├── assets/                # Các bước chính (asset) trong pipeline
    ├── jobs/                  # Các job chạy pipeline (data, model)
    ├── data/                  # Dữ liệu (raw, intermediate, final)
    └── streamlit_app/         # Giao diện người dùng
````

---

## ⚙️ Cài đặt

```bash
# 1. Clone dự án
git clone https://github.com/your-username/flood_project.git
cd flood_project

# 2. Tạo môi trường ảo (tuỳ chọn)
python -m venv venv
venv\Scripts\activate           # Trên Windows
source venv/bin/activate       # Trên macOS/Linux

# 3. Cài thư viện
pip install -r requirements.txt
```

---

## 🔁 Pipeline xử lý dữ liệu (Dagster)

Pipeline được triển khai bằng Dagster gồm các `@asset` và `@job`:

### 📦 Các asset chính (trong `assets/`)

| Tên asset                | Mô tả                                     |
| ------------------------ | ----------------------------------------- |
| `ggee_get_flood_data.py` | Lấy dữ liệu mưa từ GEOGloWS, Earth Engine |
| `data_loading.py`        | Load và chuẩn hóa dữ liệu đầu vào         |
| `preprocessing.py`       | Chuẩn hóa, tạo tensors GNN/CNN            |
| `combine_csv.py`         | Gộp dữ liệu các đợt lũ                    |
| `dbscan_clustering.py`   | Gom cụm lũ bằng DBSCAN                    |
| `water_cluster.py`       | Phân cụm vùng có nước lâu dài             |
| `training.py`            | Huấn luyện mô hình GNN-CNN-LSTM           |
| `model.py`               | Mô hình học sâu                           |
| `evaluation.py`          | Đánh giá mô hình                          |
| `predict_yenbai.py`      | Dự đoán điểm ngập trên từng ô ở Yên Bái   |
| `utils.py`               | Hàm phụ trợ dùng chung                    |

### 🧩 Các job (trong `jobs/`)

| Job            | Mục đích                            |
| -------------- | ----------------------------------- |
| `data_job.py`  | Chạy toàn bộ pipeline xử lý dữ liệu |
| `model_job.py` | Chạy huấn luyện và dự đoán mô hình  |

Chạy Dagster UI:

```bash
cd flood_pipeline
dagster dev
# Truy cập: http://localhost:3000
```

---

## 📊 Dữ liệu

### `data/raw/`

* `flood_data.csv`: Dữ liệu ngập gốc
* `diaphantinh.geojson`: Biên giới hành chính

### `data/intermediate/`

* Các file `.csv` là các đợt lũ riêng biệt và kết quả gom cụm

### `data/final/`

* `best_cnn_gnn_model.pth`: Mô hình đã huấn luyện
* `yenbai_predictions_clean.csv`: Dự đoán điểm ngập
* `water_clusters.csv`: Cụm vùng có nước

---

## 🌐 Ứng dụng Streamlit

### 1️⃣ `app.py` – **Bản đồ mưa toàn quốc**

* Hiển thị lớp thời tiết, lượng mưa từ GEOGloWS.
* Cho phép chọn khu vực và lớp bản đồ nền.
* Dành cho người dùng muốn quan sát tổng thể.

```bash
cd flood_pipeline/streamlit_app
streamlit run app.py
```

---

### 2️⃣ `rain_yenbai.py` – **Cảnh báo ngập cho Yên Bái**

* Hiển thị các ô vuông với mức độ nguy cơ ngập (`pred_flood_score`).
* Chọn ngưỡng hiển thị, hiển thị lớp địa hình, nước, cụm lũ.
* Dữ liệu đầu vào từ `yenbai_predictions_clean.csv`.

```bash
cd flood_pipeline/streamlit_app/yenbai_app
streamlit run rain_yenbai.py
```

---

## 📌 Tác giả

**Nguyễn Đăng Khôi**


