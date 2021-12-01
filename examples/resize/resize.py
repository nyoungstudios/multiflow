"""
This is an example of how one might use multiflow.

In this example:
    1. we download a folder of images from S3
    2. resize the image
    3. and save the new image to a local folder

Please change the constants right after the import statements

"""
import boto3
import json
import logging
from multiflow import MultithreadedFlow
import os
from PIL import Image
import smart_open
import sys


# Change these values before running the example
# s3 endpoint url (leave as None if not changing)
ENDPOINT = None

# s3 profile name (leave as None if using environment variables)
PROFILE = None

# s3 bucket path containing jpg images (in the format of s3://path/to/bucket/subfolder/
S3_PATH = 's3://path/to/bucket/subfolder/'

# local output directory to save rotated images
OUT_DIR = '/path/to/local/output/folder'


def get_logger(name):
    """
    Gets logger instance
    """
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
    return logging.getLogger(name)


def get_client_and_params():
    """
    Gets boto3 client and smart open's transport params
    """
    session = boto3.Session(profile_name=PROFILE)
    creds = session.get_credentials()
    transport_params = {
        'session': session,
        'resource_kwargs': {
            'endpoint_url': ENDPOINT
        }
    }
    return boto3.client(
        's3',
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        region_name=session.region_name,
        endpoint_url=ENDPOINT
    ), transport_params


def get_files_in_bucket(client, s3_path):
    """
    Recursively finds all files within an S3 bucket path
    """
    parts = s3_path[5:].rstrip('/').split('/', 1)

    if len(parts) == 1:
        bucket = parts[0]
        prefix = ''
    else:
        bucket, prefix = parts

    paginator = client.get_paginator('list_objects_v2')
    paginate_args = {'Bucket': bucket, 'Delimiter': '/'}
    if prefix:
        paginate_args['Prefix'] = prefix + '/'

    page_iterator = paginator.paginate(**paginate_args)

    for page in page_iterator:
        for content in page.get('Contents', []):
            yield os.path.join('s3://', bucket, content['Key'])

        for common_prefix in page.get('CommonPrefixes', []):
            subfolder = os.path.join('s3://', bucket, common_prefix['Prefix'], '')
            yield from get_files_in_bucket(client, subfolder)


def resize(s3_filepath, transport_params=None):
    """
    Opens s3 file path using smart open, resizes to 50%, and saves it
    """
    with smart_open.open(s3_filepath, 'rb', transport_params=transport_params) as f:
        im = Image.open(f)
        curr_w, curr_h = im.size
        new_w = round(curr_w * 0.5)
        new_h = round(curr_h * 0.5)

        im.thumbnail((new_w, new_h))

        name = os.path.basename(s3_filepath)

        out_path = os.path.join(OUT_DIR, name)
        im.save(out_path)

    return out_path


if __name__ == '__main__':
    os.makedirs(OUT_DIR, exist_ok=True)
    ct, tp = get_client_and_params()
    logger = get_logger('test')

    s3_path_to_errors = {}
    s3_path_to_out_path = {}

    with MultithreadedFlow(
        max_workers=64,
        retry_count=2,
        logger=logger,
        log_all=True
    ) as flow:
        flow.consume(get_files_in_bucket, ct, S3_PATH)
        flow.add_function(resize)

        for output in flow:
            # gets s3_filepath arg/kwarg
            s3_fp = output.get('s3_filepath')

            # there is only one function in this flow, so
            # this would be 0 representing the function resize
            fn_id = output.get_fn_id()

            # true if function is resize
            last = output.is_last()

            # gets total number of tries (max would be 3 in this case)
            num_of_tries = output.get_num_of_attempts()

            if output:  # if job was successful
                # this would print the string representation of get_result()
                # in this case, this would be the local path to the resized image
                # print(output)
                s3_path_to_out_path[s3_fp] = output.get_result()
            else:
                s3_path_to_errors[s3_fp] = output.get_exception()

        # gets total number of success and failed counts
        success_count = flow.get_successful_job_count()
        failed_count = flow.get_failed_job_count()

    print(json.dumps(s3_path_to_out_path, indent=4))
