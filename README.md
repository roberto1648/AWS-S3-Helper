# AWS S3 Helper

First open aws_helper.py in an editor and paste your access key and secret to ACCESS_KEY and SECRET, respectively.

## To get a list of buckets

list_buckets(access_key=ACCESS_KEY, secret=SECRET)


## To get a list of keys in a bucket with optional prefix and sufix in their name

e.g., 

list_keys(
        bucket_name, prefix='dir1/subdir/something', sufix='.json',
        access_key=ACCESS_KEY, secret=SECRET,
)

If there are too many keys, use instead s3_bucket_keys_generator to not load all the keys at once. These functions use pagination to find all the keys.


## Read a json object

dict = get_json_from_object(
        bucket_name, key="dir/subdir/some.json",
        access_key=ACCESS_KEY, secret=SECRET,
)


## Read a binary array

arr = get_numerical_array_from_object(
        bucket_name, key="dir/subdir/some.bin",
        access_key=ACCESS_KEY, secret=SECRET,
        res=4,
        from_to=(0, None),
)

from_to allows downloading only a slice (arr = big_array(from_to[0]: from_to[1]). Use None to refer to the begining or the end, e.g., arr[:50] -> (None, 50). Input res refers to the byte resolution of the binary file.
