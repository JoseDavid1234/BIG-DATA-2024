import sys
import re
import boto3
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, when, isnull
from pyspark.sql.types import IntegerType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuración del bucket y patrón de carpetas
source_bucket = "uefa-building"
folder_pattern = r"^\d{4}-\d{2}$"  # Formato yyyy-yy+1

# Estructura de columnas estándar
columnas_estandar = [
    'round', 'ft', 'ht', 'agg_ft', 'et', 'p', 'stage', 'group', 'day', 'week', 
    'dateonly', 'temporada', 'round_type', 'team_name_1', 'country_1', 
    'team_name_2', 'country_2', 'team1score', 'team2score', 'comments', 
    'partition_0', 'partition_1'
]

# Crear cliente de S3 usando boto3
s3 = boto3.client('s3')

# Función para limpiar solo los archivos en la subcarpeta `cleaned`
def clear_cleaned_folder(bucket, prefix):
    print(f"Clearing files in folder: s3://{bucket}/{prefix}")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if 'Contents' in response:
        delete_objects = [{'Key': obj['Key']} for obj in response['Contents']]
        s3.delete_objects(Bucket=bucket, Delete={'Objects': delete_objects})
        print(f"Deleted {len(delete_objects)} files from {prefix}")

# Obtener todas las carpetas en el bucket que cumplen con el patrón de fecha yyyy-yy+1
response = s3.list_objects_v2(Bucket=source_bucket, Prefix="", Delimiter='/')
folders = [content.get('Prefix') for content in response.get('CommonPrefixes', [])]

for folder in folders:
    folder_name = folder.strip('/')

    if re.match(folder_pattern, folder_name):
        print(f"Processing folder: {folder_name}")
        cleaned_path = f"{folder_name}/cleaned/"
        
        # Leer datos de origen antes de limpiar
        datasource = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [f"s3://{source_bucket}/{cleaned_path}"]},
            format="parquet"
        )
        
        # Convertir a DataFrame para aplicar transformaciones
        df = datasource.toDF()
        print("Columns before transformations:", df.columns)

        # Copiar `score1` y `score2` a `team1score` y `team2score` y luego eliminar `score1` y `score2`
        if 'score1' in df.columns:
            print("Copying `score1` to `team1score` and dropping `score1`")
            df = df.withColumn("team1score", when(col("score1").isNotNull(), col("score1")).otherwise(col("team1score"))).drop("score1")
        if 'score2' in df.columns:
            print("Copying `score2` to `team2score` and dropping `score2`")
            df = df.withColumn("team2score", when(col("score2").isNotNull(), col("score2")).otherwise(col("team2score"))).drop("score2")

        # Convertir `team1score` y `team2score` a IntegerType
        df = df.withColumn("team1score", col("team1score").cast(IntegerType())) \
               .withColumn("team2score", col("team2score").cast(IntegerType()))

        # Reemplazar valores nulos o "NA" en la columna `round_type` con "g"
        if 'round_type' in df.columns:
            print("Replacing null or 'NA' values in `round_type` with 'g'")
            df = df.withColumn("round_type", when(isnull(col("round_type")) | (col("round_type") == "NA"), lit("g")).otherwise(col("round_type")))

        # Normalizar columnas: agrega columnas que faltan con valores nulos
        for col_name in columnas_estandar:
            if col_name not in df.columns:
                print(f"Adding missing column: {col_name}")
                df = df.withColumn(col_name, lit(None))

        print("Columns after transformations:", df.columns)

        # Verificar si el DataFrame tiene columnas y datos antes de intentar escribir
        if len(df.columns) > 0 and df.count() > 0:
            # Limpiar los archivos existentes en la carpeta `cleaned` antes de escribir nuevos datos
            clear_cleaned_folder(source_bucket, cleaned_path)
            
            # Convertir DataFrame a DynamicFrame
            datasource_cleaned = DynamicFrame.fromDF(df, glueContext, "datasource_cleaned")

            # Escribir el DataFrame procesado en la subcarpeta `cleaned`
            glueContext.write_dynamic_frame.from_options(
                frame=datasource_cleaned,
                connection_type="s3",
                connection_options={"path": f"s3://{source_bucket}/{cleaned_path}", "partitionKeys": []},  # `partitionKeys` en blanco permite sobreescritura completa
                format="parquet",
                format_options={"compression": "SNAPPY"}  # Opcional: puede mejorar la eficiencia en almacenamiento y lectura
            )
        else:
            print(f"No data to write for folder: {folder_name}")

job.commit()

