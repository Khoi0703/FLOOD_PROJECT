from dagster import define_asset_job

model_job = define_asset_job(
    name="model_job",
    selection=[
    # ⬅️ thêm dòng này
        "water_cluster",
        "data_loading",
        "preprocessing",
        "model",
        "training",
        "evaluation",
        "yenbai_rain",  
        "predict_yenbai",
        "utils"
    ]
)
