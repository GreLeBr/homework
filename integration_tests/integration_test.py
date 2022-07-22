import os
import batch
from datetime import datetime
from pandas import Timestamp
# from deepdiff import DeepDiff
import pandas as pd



def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)

def upload_unit_df():

    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (1, 1, dt(1, 2, 0), dt(2, 2, 1)),        
    ]

    columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
    df = pd.DataFrame(data, columns=columns)
    
    # expected_df_dict = batch.prepare_data(df,columns[:2]).to_dict()
    # expected_df = batch.prepare_data(df,columns[:2])

    # expected_df = expected_df[['PUlocationID', 'DOlocationID', 'dropOff_datetime', 'duration', 'pickup_datetime']]


    # actual_df_dict = {'DOlocationID': {0: '-1', 1: '1'},
    # 'PUlocationID': {0: '-1', 1: '1'},
    # 'dropOff_datetime': {0: Timestamp('2021-01-01 01:10:00'),
    # 1: Timestamp('2021-01-01 01:10:00')},
    # 'duration': {0: 8.000000000000002, 1: 8.000000000000002},
    # 'pickup_datetime': {0: Timestamp('2021-01-01 01:02:00'),
    # 1: Timestamp('2021-01-01 01:02:00')}}

    # actual_df = pd.DataFrame.from_dict(actual_df_dict)

    # diff = DeepDiff(expected_df, actual_df)
    # print(f'diff={diff}')

    # comparison_df = expected_df.sort_index(axis=1) == actual_df.sort_index(axis=1)


    input_file = batch.get_input_path(2021, 1)

    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', "http://localhost:4566")

    options = {
    'client_kwargs': {
        'endpoint_url': S3_ENDPOINT_URL
    }
    }

    df.to_parquet(
    input_file,
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=options
    )

    # assert expected_df_dict == actual_df_dict

    # # print( batch.prepare_data(df,columns[:2]))
    # print( pd.DataFrame.from_dict(actual_df))

def test_written_data():


    os.system("python batch.py 2021 1")
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN')
    output_file = output_pattern.format(year=2021, month=1)
    df_result = batch.read_data(output_file)
    actual_y_pred_sum = df_result['predicted_duration'].sum() 
    expected_y_pred_sum = 69.28869683240714


    assert actual_y_pred_sum == expected_y_pred_sum

if __name__ == '__main__':
    upload_unit_df()
    test_written_data()