import dlt
import pandas as pd
from pathlib import Path
import os

# definint resourse 
@dlt.resource(write_disposition="append")
def load_json_resource(file_path: str, **kwargs):
    df = pd.read_json(file_path, **kwargs)
    yield df.to_dict(orient="records")

if __name__ == "__main__":
    working_directory = Path(__file__).parent
    os.chdir(working_directory)

    # pipeline definition
    pipeline = dlt.pipeline(
        pipeline_name="load_snowflake",
        destination="snowflake",
        dataset_name="landing_historical",  # schema
    )


    # running the pipeline while iterating through the files
    bucket_path = os.path.join(working_directory, "downloads")
    for file in os.listdir(bucket_path):
        print("Loading file" ,file)

        try:

            if file.endswith('.json'): 
                data = list(
                    load_json_resource(
                        os.path.join(bucket_path,file) , encoding="latin1"
                    )
                )
                load_info = pipeline.run(data, table_name="events")
                print(load_info)
        except Exception as e:
          print(f"Failed to process {file}: {e}")

        
            