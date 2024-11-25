import sys
import boto3
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuración de buckets
source_bucket = 'uefa-building'
dest_bucket = 'uefa-master'
dest_file_name = 'combined_data.csv'
s3_client = boto3.client('s3')

def list_cleaned_folders(bucket):
    """
    Lista las carpetas que siguen la nomenclatura yyyy-yy+1 y contienen una subcarpeta 'cleaned'.
    """
    logger.info(f"Listando carpetas en el bucket {bucket}")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix='', Delimiter='/')
    year_folders = [
        prefix['Prefix'] for prefix in response.get('CommonPrefixes', [])
        if prefix['Prefix'][:-1].isdigit() and 'cleaned/' in prefix['Prefix']
    ]

    logger.info(f"Carpetas detectadas: {year_folders}")
    return year_folders

def read_parquet_from_folders(bucket, folders):
    """
    Lee archivos Parquet desde las carpetas especificadas.
    """
    logger.info(f"Leyendo archivos Parquet de las carpetas: {folders}")
    dataframes = []

    for folder in folders:
        folder_path = f"s3://{bucket}/{folder}"
        try:
            logger.info(f"Leyendo archivos desde: {folder_path}")
            df = spark.read.parquet(folder_path)
            dataframes.append(df)
        except Exception as e:
            logger.error(f"Error al leer los datos de {folder_path}: {e}")

    if not dataframes:
        logger.warning("No se encontraron datos en las carpetas especificadas.")
        return None

    # Fusionar todos los DataFrames
    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.unionByName(df)

    return combined_df

def write_to_csv(df, bucket, file_name):
    """
    Escribe un DataFrame en formato CSV en un bucket S3.
    """
    if df is None or df.rdd.isEmpty():
        logger.warning("El DataFrame está vacío. No se generará ningún archivo CSV.")
        return

    dest_path = f"s3://{bucket}/{file_name}"
    try:
        logger.info(f"Guardando los datos combinados como CSV en: {dest_path}")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(dest_path)
        logger.info("Archivo CSV guardado correctamente.")
    except Exception as e:
        logger.error(f"Error al guardar el archivo CSV: {e}")

def main():
    # Listar carpetas con la nomenclatura yyyy-yy+1 y subcarpeta 'cleaned'
    cleaned_folders = list_cleaned_folders(source_bucket)

    if not cleaned_folders:
        logger.warning("No se encontraron carpetas con datos para procesar.")
        return

    # Leer y fusionar los datos desde las carpetas detectadas
    combined_df = read_parquet_from_folders(source_bucket, cleaned_folders)

    # Guardar los datos combinados en un único archivo CSV
    write_to_csv(combined_df, dest_bucket, dest_file_name)

if __name__ == "__main__":
    main()

job.commit()
