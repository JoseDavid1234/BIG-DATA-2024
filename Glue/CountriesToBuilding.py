import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


source_bucket = 'uefa-raw'
source_folder = 'COUNTRIES/'
dest_bucket = 'uefa-building'
dest_folder = 'COUNTRIES/'


s3_client = boto3.client('s3')

def delete_existing_files(bucket, folder):
    """Elimina todos los archivos en una carpeta específica del bucket."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)
    
    if 'Contents' in response:
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        if objects_to_delete:
            s3_client.delete_objects(Bucket=bucket, Delete={'Objects': objects_to_delete})

def ensure_folder_exists(bucket, folder):
    """Crea la carpeta si no existe en el bucket de destino."""
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder, Delimiter='/')
    if 'CommonPrefixes' not in response:
        # Crea un objeto vacío para asegurarse de que la carpeta exista
        s3_client.put_object(Bucket=bucket, Key=(folder + '/'))

def process_and_save_data():
    """Procesa el archivo Parquet, modifica las columnas y guarda el resultado."""
    # Ruta de origen y destino
    source_path = f's3://{source_bucket}/{source_folder}'
    dest_path = f's3://{dest_bucket}/{dest_folder}'
    
    # Carga el archivo Parquet en un DataFrame de Spark
    df = spark.read.parquet(source_path)
    
    # Modifica el DataFrame: elimina columna 'id' y renombra 'fifa_name' a 'country_id'
    df_modified = df.drop('id').withColumnRenamed('fifa_name', 'country_id')
    
    # Verifica y prepara la carpeta de destino
    ensure_folder_exists(dest_bucket, dest_folder)
    delete_existing_files(dest_bucket, dest_folder)
    
    # Guarda el DataFrame modificado en formato Parquet en la carpeta de destino
    df_modified.write.mode('overwrite').parquet(dest_path)

def main():
    process_and_save_data()

if __name__ == "__main__":
    main()

job.commit()
