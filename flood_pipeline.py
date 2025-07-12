from dagster import Definitions

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
        model_job
    ]
)
