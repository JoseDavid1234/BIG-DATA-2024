import sys
import boto3
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

source_bucket = 'uefa-landing'  # Bucket de origen
dest_bucket = 'uefa-raw'        # Bucket de destino
s3_client = boto3.client('s3')

def list_season_folders(bucket):
    """Lista carpetas en el bucket que cumplen el formato yyyy-yy+1."""
    response = s3_client.list_objects_v2(Bucket=bucket, Delimiter='/')
    matching_folders = []

    if 'CommonPrefixes' in response:
        for prefix in response['CommonPrefixes']:
            folder_name = prefix['Prefix'].strip('/')
            if re.match(r'^\d{4}-\d{2}$', folder_name):
                matching_folders.append(folder_name)
    
    return matching_folders

def delete_existing_folder(bucket, folder):
    """Elimina todos los archivos en una carpeta específica del bucket."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)
    if 'Contents' in response:
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        if objects_to_delete:
            s3_client.delete_objects(Bucket=bucket, Delete={'Objects': objects_to_delete})

def process_season_folder(folder):
    """Procesa una carpeta con el formato yyyy-yy+1."""
    delete_existing_folder(dest_bucket, folder)

    source_path = f's3://{source_bucket}/{folder}/'
    df = glueContext.create_dynamic_frame.from_options(
        's3',
        {'paths': [source_path], 'recurse': True},
        format='csv',
        format_options={"withHeader": True}    
    ).toDF()
    
    dest_path = f's3://{dest_bucket}/{folder}/'
    df.write.mode("overwrite").parquet(dest_path)

def main():
    season_folders = list_season_folders(source_bucket)
    for folder in season_folders:
        process_season_folder(folder)

if __name__ == "__main__":
    main()

job.commit()
