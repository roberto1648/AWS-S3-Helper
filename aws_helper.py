import numpy as np
import pandas as pd
import boto3
import json


ACCESS_KEY = "your access key"
SECRET = "your secret"


def get_s3_client(access_key=ACCESS_KEY, secret=SECRET):
    session = boto3.session.Session(access_key, secret)
    client = session.client("s3")
    return client


def list_buckets(access_key=ACCESS_KEY, secret=SECRET):
    client = get_s3_client(access_key, secret)
    return [x['Name'] for x in client.list_buckets()['Buckets']]


def list_keys(
        bucket_name, prefix='', sufix='',
        access_key=ACCESS_KEY, secret=SECRET,
):
    return list(s3_bucket_keys_generator(bucket_name, prefix, sufix, access_key, secret))


def s3_bucket_keys_generator(
        bucket_name, prefix='', sufix='',
        access_key=ACCESS_KEY, secret=SECRET,
):
    client = get_s3_client(access_key, secret)

    # Create a reusable Paginator
    paginator = client.get_paginator('list_objects')

    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(Bucket=bucket_name)

    for page in page_iterator:
        for obj in page['Contents']:
            key = obj["Key"]
            if key.startswith(prefix) and key.endswith(sufix): 
                yield key


def ls_s3( # like bash's ls, print the keys in bucket with prefix and sufix
    bucket_name, prefix='', sufix='',
    access_key=ACCESS_KEY, secret=SECRET,
):
    for key in s3_bucket_keys_generator(bucket_name, prefix, sufix, access_key, secret):
        print(key)


def get_object_contents_string(
        bucket_name, key,
        access_key=ACCESS_KEY, secret=SECRET,
        bytes_from_to=(0, None), # None for begin or end, e.g., str[:50] -> (None, 50)
):
    client = get_s3_client(access_key, secret)

    if (bytes_from_to[0] is not None) or (bytes_from_to[1] is not None):
        ri = str(int(bytes_from_to[0])) if bytes_from_to[0] is not None else ""
        rf = str(int(bytes_from_to[1])) if bytes_from_to[1] is not None else ""
        range_str = f"bytes={ri}-{rf}"
        obj = client.get_object(Bucket=bucket_name, Key=key, Range = range_str)
    else:
        obj = client.get_object(Bucket=bucket_name, Key=key)

    return obj["Body"].read()#.decode()


def get_json_from_object(
        bucket_name, key,
        access_key=ACCESS_KEY, secret=SECRET,
):
    obj = get_object_contents_string(bucket_name, key, access_key, secret)
    return json.loads(obj)


def get_numerical_array_from_object(
        bucket_name, key,
        access_key=ACCESS_KEY, secret=SECRET,
        res=4, # bytes
        from_to=(0, None), # None for begin or end, e.g., arr[:50] -> (None, 50)
):
    if (from_to[0] is not None) or (from_to[1] is not None):
        ri = int(from_to[0] * res) if from_to[0] is not None else None
        rf = int(from_to[1] * res) if from_to[1] is not None else None
        obj = get_object_contents_string(bucket_name, key, access_key, secret,
                                         bytes_from_to = (ri, rf))
    else:
        obj = get_object_contents_string(bucket_name, key, access_key, secret)

    # make sure its multiple of res long:
    nover = np.remainder(len(obj), int(res))

    return np.fromstring(obj[:-nover], dtype=f"<f{res}")#.view(np.complex64)


def get_complex_numerical_array_from_object(
        bucket_name, key,
        access_key=ACCESS_KEY, secret=SECRET,
        res=4, # bytes
        from_to=(0, None), # None for begin or end, e.g., arr[:50] -> (None, 50)
):
    ri = int(from_to[0] * 2) if from_to[0] is not None else None
    rf = int(from_to[1] * 2) if from_to[1] is not None else None
    np_arr = get_numerical_array_from_object(bucket_name, key, access_key, secret, res, (ri, rf))
    return np_arr.view(np.complex64)


def df_from_csv_object(
        bucket_name, key,
        access_key=ACCESS_KEY, secret=SECRET,
):
    client = get_s3_client(access_key, secret)
    obj = client.get_object(Bucket = bucket_name, Key = key)
    return pd.read_csv(obj['Body'], header=None)


