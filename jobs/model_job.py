from dagster import define_asset_job

model_job = define_asset_job(
    name="model_job",
    selection=[
        "water_cluster",
        "data_loading",
        "preprocessing",
        "model",
        "training",
        "evaluation",
        "predict_yenbai",
        "utils"
    ]
)
