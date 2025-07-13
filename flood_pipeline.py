from dagster import Definitions, ScheduleDefinition

from assets.run_similar import run_similar
from assets.ggee_get_flood_data import fetch_flood_data
from assets.combine_csv import combine_csv
from assets.water_cluster import water_cluster
from assets.model import model
from assets.data_loading import data_loading
from assets.preprocessing import preprocessing
from assets.training import training
from assets.evaluation import evaluation
from assets.utils import utils
from assets.predict_yenbai import predict_yenbai
from assets.yenbai_rain import yenbai_rain 
from jobs.model_job import model_job
from jobs.data_job import data_job
from dagster import define_asset_job

# Schedule: Run data_job once a month (1st day at midnight, Vietnam time)
data_job_monthly_schedule = ScheduleDefinition(
    job=data_job,
    cron_schedule="0 0 1 * *",  # 1st day of month at 00:00
    name="data_job_monthly_schedule",
    execution_timezone="Asia/Ho_Chi_Minh"
)

# Schedule: Run water_cluster asset daily (create a job if needed)
water_cluster_job = define_asset_job("water_cluster_job", selection=["water_cluster"])
water_cluster_daily_schedule = ScheduleDefinition(
    job=water_cluster_job,
    cron_schedule="0 0 * * *",  # Every day at midnight
    name="water_cluster_daily_schedule",
    execution_timezone="Asia/Ho_Chi_Minh"
)


# Schedule: Run predict_yenbai asset daily
predict_yenbai_job = define_asset_job("predict_yenbai_job", selection=["predict_yenbai"])
predict_yenbai_daily_schedule = ScheduleDefinition(
    job=predict_yenbai_job,
    cron_schedule="0 0 * * *",  # Every day at midnight
    name="predict_yenbai_daily_schedule",
    execution_timezone="Asia/Ho_Chi_Minh"
)

defs = Definitions(
    assets=[
        run_similar,
        fetch_flood_data,
        combine_csv,
        water_cluster,
        data_loading,
        preprocessing,
        model,
        training,
        evaluation,
        utils,
        yenbai_rain,
        predict_yenbai
    ],
    jobs=[
        data_job,
        model_job,
        water_cluster_job,
        predict_yenbai_job
    ],
    schedules=[
        data_job_monthly_schedule,
        water_cluster_daily_schedule,
        predict_yenbai_daily_schedule
    ]
)
