from dagster import define_asset_job

data_job = define_asset_job(
    name="data_job",
    selection=[
        "run_similar",
        "fetch_flood_data",
        "combine_csv",
    ]
)
