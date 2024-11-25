import sys
import boto3
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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
source_bucket = 'uefa-raw'
dest_bucket = 'uefa-master'

# Conexión con S3
s3_client = boto3.client('s3')

def delete_existing_folder(bucket, folder):
    """Elimina todos los archivos en una carpeta específica del bucket."""
    logger.info(f"Eliminando carpeta existente: {folder} en bucket: {bucket}")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)
    if 'Contents' in response:
        objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
        if objects_to_delete:
            s3_client.delete_objects(Bucket=bucket, Delete={'Objects': objects_to_delete})
            logger.info(f"Carpeta eliminada: {folder}")
        else:
            logger.warning(f"No se encontraron objetos para eliminar en {folder}")
    else:
        logger.warning(f"No se encontró la carpeta {folder} en el bucket {bucket}")

def process_data(source_folder, destination_folder):
    """Procesa los datos desde una carpeta de origen y los escribe como Parquet en una carpeta de destino."""
    folder = f"GeneralData/{source_folder}/"
    logger.info(f"Procesando carpeta: {folder}")

    # Eliminar los datos existentes en la carpeta de destino
    delete_existing_folder(dest_bucket, destination_folder)

    # Listar archivos en la carpeta de origen
    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=folder)
    files = [f"s3://{source_bucket}/{obj['Key']}" for obj in response.get('Contents', []) if obj['Key'].endswith(('.csv', '.json'))]

    if not files:
        logger.warning(f"No se encontraron archivos en {folder}")
        return

    # Crear un DataFrame para procesar los archivos
    dataframes = []
    for file_path in files:
        try:
            if file_path.endswith(".csv"):
                logger.info(f"Leyendo archivo CSV: {file_path}")
                df = spark.read.option("header", "true").csv(file_path)
            elif file_path.endswith(".json"):
                logger.info(f"Leyendo archivo JSON: {file_path}")
                df = spark.read.option("multiline", "true").json(file_path)
            else:
                logger.warning(f"Formato de archivo no soportado: {file_path}")
                continue

            dataframes.append(df)

        except Exception as e:
            logger.error(f"Error al procesar el archivo {file_path}: {e}")

    if not dataframes:
        logger.warning(f"No se procesaron archivos en {folder}")
        return

    # Unir todos los DataFrames en uno solo
    try:
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.unionByName(df)

        # Guardar en formato Parquet en la carpeta de destino
        dest_path = f"s3://{dest_bucket}/{destination_folder}"
        combined_df.coalesce(1).write.mode("overwrite").parquet(dest_path)
        logger.info(f"Datos guardados correctamente en: {dest_path}")

    except Exception as e:
        logger.error(f"Error al guardar los datos en la carpeta {destination_folder}: {e}")

def main():
    # Procesar TEAMS y guardar en DimTeams
    process_data(source_folder="TEAMS", destination_folder="DimTeams/")

    # Procesar COUNTRIES y guardar en DimCountries
    process_data(source_folder="COUNTRIES", destination_folder="DimCountries/")

if __name__ == "__main__":
    main()

job.commit()
