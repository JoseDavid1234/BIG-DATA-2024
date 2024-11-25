import sys
import boto3
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame  # Asegúrate de importar DynamicFrame
from pyspark.sql.functions import col, lower, regexp_replace, trim, lit, split
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

try:
    # Leer los archivos Parquet desde S3
    source_path = "s3://uefa-raw/GeneralData/TEAMS"  # Ruta a los archivos Parquet en el bucket de S3
    logger.info(f"Leyendo datos desde: {source_path}")
    df = spark.read.parquet(source_path)

    # Eliminar la columna 'index'
    df = df.drop("index")

    # Separar la columna 'total_goals' en dos columnas
    df = df.withColumn("total_goals_scored", split(df["total_goals"], "-")[0].cast("int"))
    df = df.withColumn("total_goals_against", split(df["total_goals"], "-")[1].cast("int"))

    # Convertir el DataFrame de Spark a un DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

    # Escribir el resultado en S3 como un archivo Parquet
    target_path = "s3://uefa-master/DimTeams"
    logger.info(f"Escribiendo datos en: {target_path}")
    glueContext.write_dynamic_frame.from_options(
        dynamic_frame,
        connection_type="s3",
        connection_options={"path": target_path},
        format="parquet"
    )

    # Finalizar el job
    job.commit()
    logger.info("Job completado con éxito.")

except Exception as e:
    logger.error(f"Ocurrió un error: {str(e)}")
    job.commit()
    raise
