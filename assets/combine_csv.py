from dagster import asset
import pandas as pd
import glob

@asset
def combine_csv(context, fetch_flood_data: pd.DataFrame) -> pd.DataFrame:
    input_files = glob.glob("data/intermediate/event_data/data_*.csv")
    if not input_files:
        context.log.warning("⚠️ Không tìm thấy file để combine.")
        return pd.DataFrame()

    df_list = [pd.read_csv(f) for f in input_files]
    combined_df = pd.concat(df_list, ignore_index=True)

    output_path = "data/intermediate/combined_flood_event_data.csv"
    combined_df.to_csv(output_path, index=False)
    context.log.info(f"✅ Combined {len(input_files)} files into {output_path}")

    return combined_df
