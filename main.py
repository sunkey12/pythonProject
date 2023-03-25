import logging
import uuid
import boto3
from botocore.exceptions import ClientError

# Connect to the low-level client interface
s3_client = boto3.client('s3')

# Connect to the high-level interface
s3_resource = boto3.resource('s3')


# Functions:

# Function to create bucket name
# Function return bucket name if it's worked and else none
def create_bucket_name(bucket_prefix):
    # Try to create bucket name
    try:
        # The generated bucket name must be between 3 and 63 chars long
        return ''.join([bucket_prefix, str(uuid.uuid4())])

    # Catch the exception and write log
    except ClientError as e:
        logging.error(e)
        return None


# Function to create bucket
# Function return bucket_response and bucket_name if it's worked and else 'error'
def create_bucket(bucket_prefix, s3_connection):
    # Try to add bucket
    try:
        session = boto3.session.Session()
        current_region = session.region_name
        bucket_name = create_bucket_name(bucket_prefix)
        bucket_response = s3_connection.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': current_region})
        print(bucket_name, current_region)

    # Catch the exception and write log
    except ClientError as e:
        logging.error(e)
        return 'error'
    return bucket_response, bucket_name


# Function to create temp file by getting size, file_name and file_content
# Function return random_file_name if it's worked and else 'error'
def create_temp_file(size, file_name, file_content):
    # Try to create temp file
    try:
        random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
        with open(random_file_name, 'w') as f:
            f.write(str(file_content) * size)

    # Catch the exception and write log
    except ClientError as e:
        logging.error(e)
        return 'error'
    return random_file_name


# Function that getting bucket_from_name and bucket_to_name and file_name and copy to bucket
def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    # Try to create copy
    try:
        copy_source = {
            'Bucket': bucket_from_name,
            'Key': file_name
        }

    # Catch the exception and write log
    except ClientError as e:
        logging.error(e)
    s3_resource.Object(bucket_to_name, file_name).copy(copy_source)


# Function to enable bucket versioning by getting bucket_name
def enable_bucket_versioning(bucket_name):
    # Try to enable and print status
    try:
        bkt_versioning = s3_resource.BucketVersioning(bucket_name)
        bkt_versioning.enable()
        print(bkt_versioning.status)

    # Catch the exception and write log
    except ClientError as e:
        logging.error(e)


# Function to delete all objects by getting bucket_name
def delete_all_objects(bucket_name):
    # Try to delete
    try:
        res = []
        bucket = s3_resource.Bucket(bucket_name)
        for obj_version in bucket.object_versions.all():
            res.append({'Key': obj_version.object_key,
                        'VersionId': obj_version.id})
        print(res)
        bucket.delete_objects(Delete={'Objects': res})

    # Catch the exception and write log
    except ClientError as e:
        logging.error(e)


# Main:

if __name__ == "__main__":

    # Creating two buckets, the first with a client connection type and the second with a resource connection type
    # By calling create_bucket function
    first_bucket_name, first_response = create_bucket(
        bucket_prefix='first_bucket', s3_connection=s3_resource.meta.client)

    second_bucket_name, second_response = create_bucket(
        bucket_prefix='second_bucket', s3_connection=s3_resource)

    # Creating a file by calling to the create_temp_file function
    first_file_name = create_temp_file(300, 'firstFile.txt', 'ArikFirstFile')

    # Uploading the first file to the first bucket
    # By calling upload file function
    first_bucket = s3_resource.Bucket(name=first_bucket_name)
    first_object = s3_resource.Object(
        bucket_name=first_bucket_name, key=first_file_name)
    first_object.upload_file(first_file_name)

    # Downloading the first file that we just uploaded by calling download_file function
    s3_resource.Object(first_bucket_name, first_file_name).download_file(
        f'/tmp/{first_file_name}')

    # Copying the first file from the first bucket to the second bucket by calling copy_to_bucket function
    copy_to_bucket(first_bucket_name, second_bucket_name, first_file_name)

    # Deleting the file we just copied to the second bucket by calling delete function
    s3_resource.Object(second_bucket_name, first_file_name).delete()

    # Creating a new file in the first bucket and making it public
    # By calling create_temp_file and upload_file functions
    second_file_name = create_temp_file(400, 'secondFile.txt', 'ArikSecondFile')
    second_object = s3_resource.Object(first_bucket.name, second_file_name)
    second_object.upload_file(second_file_name, ExtraArgs={
        'ACL': 'public-read'})

    # Loading the file's ACL into a variable and print grants
    second_object_acl = second_object.Acl()
    print(second_object_acl.grants)

    # Changing the file's premissions to privte again and print grants
    response = second_object_acl.put(ACL='private')
    print(second_object_acl.grants)

    # Creating another file in the first bucket and encrypting it and print server_side_encryption
    third_file_name = create_temp_file(300, 'thirdFile.txt', 'ArikThirdFile')
    third_object = s3_resource.Object(first_bucket_name, third_file_name)
    third_object.upload_file(third_file_name, ExtraArgs={
        'ServerSideEncryption': 'AES256'})
    print(third_object.server_side_encryption)

    # Re-uploading the third file again but as a different storage class and print storage_class
    third_object.upload_file(third_file_name, ExtraArgs={
        'ServerSideEncryption': 'AES256',
        'StorageClass': 'STANDARD_IA'})
    third_object.reload()
    print(third_object.storage_class)

    # Enables versioning of files in the first
    enable_bucket_versioning(first_bucket_name)

    s3_resource.Object(first_bucket_name, second_file_name).upload_file(
        second_file_name)
    print(s3_resource.Object(first_bucket_name, first_file_name).version_id)

    # Printing all bucket names using the s3 resource by using for loop
    for bucket in s3_resource.buckets.all():
        print(bucket.name)

    # Printing all bucket names using the s3 client by using for loop
    for bucket_dict in s3_resource.meta.client.list_buckets().get('Buckets'):
        print(bucket_dict['Name'])

    # Two different methods for printing all the file names in the first bucket by using for loops
    for obj in first_bucket.objects.all():
        print(obj.key)

    for obj in first_bucket.objects.all():
        sub_src = obj.Object()
        print(obj.key, obj.storage_class, obj.last_modified,
              sub_src.version_id, sub_src.metadata)

    # Calling a previously configured function that deletes all files in the bucket
    delete_all_objects(first_bucket_name)

    # Uploading an already existing file to the second bucket
    s3_resource.Object(second_bucket_name, first_file_name).upload_file(
        first_file_name)
    delete_all_objects(second_bucket_name)

    # Deleting the first and the second buckets by using delete function
    s3_resource.Bucket(first_bucket_name).delete()
    s3_resource.meta.client.delete_bucket(Bucket=second_bucket_name)
