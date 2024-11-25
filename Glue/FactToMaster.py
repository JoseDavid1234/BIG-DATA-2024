import sys
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Inicialización del contexto
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Nombre del bucket origen
source_bucket = "uefa-building"
source_prefix = ""  # Si quieres todo desde la raíz, dejar vacío

# Nombre del bucket destino
target_bucket = "uefa-master"
target_prefix = "FactMatches/"

# Ruta completa de origen y destino
source_path = f"s3://{source_bucket}/{source_prefix}"
target_path = f"s3://{target_bucket}/{target_prefix}"

# Cargar los archivos Parquet desde el bucket origen
print(f"Cargando datos desde {source_path}")
input_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [source_path], "recurse": True},
    format="parquet"
)

# Convertir DynamicFrame a DataFrame
input_dataframe = input_dynamic_frame.toDF()

# Escribir los datos consolidados en el bucket destino en formato Parquet
print(f"Escribiendo datos consolidados en {target_path}")
input_dataframe.write.mode("overwrite").parquet(target_path)

print("Proceso finalizado.")
