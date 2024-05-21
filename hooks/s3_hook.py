# See the link below for the official documentation of boto3 S3 resource
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

import os
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook as AirflowS3Hook
from botocore.exceptions import ClientError

class S3Hook:
    """

    Creates a hook to a specific S3 bucket.
    You should specify the bucket name when initializing this object.

    """

    def __init__(self, bucket_name):
        self.hook = AirflowS3Hook()
        self.s3_bucket = self.hook.get_bucket(bucket_name)

    def upload_file(self, file_path, key):
        """

        It uploads a file to a s3 bucket

        Parameters:

            - file_path(str): path where file is located
            - key(str): S3 key that will point to the file.
                        Should be in format path/to/<my_file>.<extension> 
        
        """

        if not(os.path.exists(file_path)):
            raise AirflowException("File does not exists.")
        
        self.s3_bucket.upload_file(file_path, key)

    def download_file(self, key, file_path):
        """

        It downloads a file from a s3 bucket 
        
        Parameters:

            - key(str): key to the file in the s3 bucket
            - file_path(str): path where the file will be located after it was downloaded

        """

        try:
            self.s3_bucket.download_file(key, file_path)

        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                raise AirflowException("The object does not exists.")
            else:
                raise

    def delete_file(self, objlist):
        """

        Delete multiple objects from an Amazon S3 Bucket

        Parametes:

            - objlist(list): list of objects to delete from the S3 bucket.
                             Each element of the list should be the key
                             to the file you want to delete.
        
        """

        objects_to_delete = []
        for obj in objlist:
            objects_to_delete.append({'Key': obj})

        self.s3_bucket.delete_objects(Delete={
            'Objects': objects_to_delete
        })

    def list_files_from_bucket(self, filter_by_ext=''):
        """

        Read all files from `self.s3_bucket`

        Parameters:

            - filter_by_ext(optional): return only files with provided extension. Eg: '.csv' '.txt'
            
        """

        s3_files = self.s3_bucket.objects.all()
        parsed_files = []

        for s3_file in s3_files:

            file_info = Path(s3_file.key)

            file_ext = file_info.suffix
            file_name = file_info.stem

            if filter_by_ext:
                if not file_ext.lower() == filter_by_ext.lower():
                    continue

            parsed_files.append({
                'file_name': f'{file_name}{file_ext}',
                'file_path': s3_file.key,
                'last_modified': s3_file.last_modified
            })

        return parsed_files
