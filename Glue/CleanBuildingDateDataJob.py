import sys
import re
import boto3
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import split, col, regexp_replace, lit, to_date, regexp_extract, date_format, weekofyear, substring
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

        # Procesar la columna `datetime` si existe y obtener solo la fecha en `dateonly`
        if 'datetime' in df.columns:
            print("Processing `datetime` column for `dateonly`")
            df = df.withColumn('dateonly', date_format(to_date(col('datetime')), 'yyyy-MM-dd'))
            df = df.drop('datetime')
        
        # Si no existe `datetime`, procesar `date` si está disponible
        elif 'Date' in df.columns:
            print("Processing `date` column for transformations")
            # Extraer `day`, `week`, y `dateonly`
            df = df.withColumn("day", regexp_extract(col("date"), r'\((\w{3})\)', 1)) \
                   .withColumn("week", regexp_extract(col("date"), r'\(W(\d{1,2})\)', 1).cast(IntegerType())) \
                   .withColumn('dateonly', date_format(to_date(regexp_extract(col('date'), r'(\d{1,2} \w{3} \d{4})', 1), 'd MMM yyyy'), 'yyyy-MM-dd')) \
                   .drop('date')
        else:
            print("No `date` or `datetime` column found in this DataFrame")

        # Si `dateonly` está presente, extraer `day` y `week`
        if 'dateonly' in df.columns:
            df = df.withColumn("day", substring(date_format(col("dateonly"), "EEEE"), 1, 3)) \
                   .withColumn("week", weekofyear(col("dateonly")))

        # Eliminar las columnas `∑ft` y `matchday` si están presentes
        columns_to_drop = ['∑ft', 'matchday','index']
        for col_name in columns_to_drop:
            if col_name in df.columns:
                print(f"Dropping column: {col_name}")
                df = df.drop(col_name)

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

