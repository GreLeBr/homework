import batch
from datetime import datetime
from pandas import Timestamp
# from deepdiff import DeepDiff
import pandas as pd


def test_simple():
    assert 1 == 1

def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)

def test_prepare_data():


    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (1, 1, dt(1, 2, 0), dt(2, 2, 1)),        
    ]

    columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
    df = pd.DataFrame(data, columns=columns)
    
    expected_df = batch.prepare_data(df,columns[:2]).to_dict()

    actual_df = {'DOlocationID': {0: '-1', 1: '1'},
    'PUlocationID': {0: '-1', 1: '1'},
    'dropOff_datetime': {0: Timestamp('2021-01-01 01:10:00'),
    1: Timestamp('2021-01-01 01:10:00')},
    'duration': {0: 8.000000000000002, 1: 8.000000000000002},
    'pickup_datetime': {0: Timestamp('2021-01-01 01:02:00'),
    1: Timestamp('2021-01-01 01:02:00')}}

    # diff = DeepDiff(expected_df, actual_df)
    # print(f'diff={diff}')
    assert expected_df == actual_df

    # # print( batch.prepare_data(df,columns[:2]))
    # print( pd.DataFrame.from_dict(actual_df))

if __name__ == '__main__':
    test_prepare_data()