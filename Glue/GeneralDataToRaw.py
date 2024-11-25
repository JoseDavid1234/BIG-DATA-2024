import sys
import boto3
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lower, regexp_replace, trim, lit
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

source_bucket = 'uefa-landing'
dest_bucket = 'uefa-raw'
s3_client = boto3.client('s3')

schemas = {
    "TEAMS": {
        "name": "string",
        "foundation_year": "int",
        "status": "string"
    },
    "COUNTRIES": {
        "country_name": "string",
        "code": "string",
        "region": "string"
    },
    "PLAYERS": {
        "player_name": "string",
        "team": "string",
        "matches_played": "int",
        "goals": "int"
    }
}

def normalize_name_automatic(df, col_name="name"):
    logger.info("Datos antes de normalizar la columna 'name':")
    df.select(col_name).show(5, truncate=False)
    """Normaliza nombres automáticamente."""
    logger.info(f"Normalizando columna: {col_name}")
    if col_name in df.columns:
        df = df.withColumn(
            col_name,
            trim(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower(col(col_name)),
                            r"\b(sc|bc|acf|afc|fc|cf|club|de|la|fútbol|football|04)\b",  # Palabras completas a eliminar
                            ""  # Reemplazar por vacío
                        ),
                        r"[^a-z\s]",  # Elimina caracteres especiales
                        ""  # Reemplazar por vacío
                    ),
                    r"\s+",  # Reemplaza múltiples espacios consecutivos
                    " "  # Por un solo espacio
                )
            )
        )
        logger.info(f"Datos después de normalizar la columna '{col_name}':")
        df.select(col_name).show(5, truncate=False)
    else:
        logger.warning(f"La columna '{col_name}' no existe en el DataFrame.")
    return df


def align_to_schema(df, schema):
    """Alinea un DataFrame al esquema base."""
    logger.info(f"Alineando DataFrame al esquema: {schema}")
    for col_name, col_type in schema.items():
        if col_name not in df.columns:
            logger.warning(f"Columna faltante: {col_name}. Añadiendo con valor nulo.")
            df = df.withColumn(col_name, lit(None).cast(col_type))
        else:
            df = df.withColumn(col_name, df[col_name].cast(col_type))
    logger.info(f"Esquema del DataFrame después de alinear:")
    df.printSchema()
    return df

def unify_schemas(dfs):
    """Unifica los esquemas de múltiples DataFrames."""
    logger.info("Unificando esquemas de DataFrames...")
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)
    unified_dfs = []
    for df in dfs:
        for col in all_columns:
            if col not in df.columns:
                logger.warning(f"Añadiendo columna faltante: {col}")
                df = df.withColumn(col, lit(None))
        unified_dfs.append(df)
    logger.info("Esquemas unificados correctamente.")
    return unified_dfs

def merge_team_data(files):
    """
    Fusiona los datos de equipos desde múltiples archivos sin perder columnas y consolidando filas con la misma clave.
    """
    logger.info(f"Iniciando la fusión de datos para los archivos: {files}")
    dataframes = []

    for file_path in files:
        try:
            if file_path.endswith(".csv"):
                logger.info(f"Procesando archivo CSV: {file_path}")
                schema = StructType([
                    StructField("index", IntegerType(), True),
                    StructField("Team", StringType(), True),
                    StructField("M.", IntegerType(), True),
                    StructField("W", IntegerType(), True),
                    StructField("D", IntegerType(), True),
                    StructField("L", IntegerType(), True),
                    StructField("goals", StringType(), True),
                    StructField("Dif", IntegerType(), True),
                    StructField("Pt.", IntegerType(), True)
                ])
                df = spark.read.option("header", "true").schema(schema).csv(file_path)
                df = df.withColumnRenamed("Team", "name") \
                       .withColumnRenamed("M.", "matches_played") \
                       .withColumnRenamed("W", "wins") \
                       .withColumnRenamed("D", "draws") \
                       .withColumnRenamed("L", "losses") \
                       .withColumnRenamed("goals", "total_goals") \
                       .withColumnRenamed("Dif", "goal_difference") \
                       .withColumnRenamed("Pt.", "points")
                df = normalize_name_automatic(df, "name")

            elif file_path.endswith(".json"):
                logger.info(f"Procesando archivo JSON: {file_path}")
                df_raw = spark.read.option("multiline", "true").json(file_path)
                if df_raw.rdd.isEmpty():
                    logger.warning(f"El DataFrame cargado desde el JSON {file_path} está vacío.")
                    continue
                if "coach" in df_raw.columns:
                    df_raw = df_raw \
                        .withColumn("coach_first_name", col("coach.first_name")) \
                        .withColumn("coach_last_name", col("coach.last_name")) \
                        .withColumn("coach_nationality", col("coach.nationality")) \
                        .withColumn("coach_date_of_birth", col("coach.date_of_birth")) \
                        .drop("coach")
                df = df_raw.withColumnRenamed("founded", "foundation_year")
                df = normalize_name_automatic(df, "name")
            else:
                logger.error(f"Formato de archivo no soportado: {file_path}")
                continue

            df = align_to_schema(df, schemas.get("TEAMS", {}))
            dataframes.append(df)

        except Exception as e:
            logger.error(f"Error al procesar el archivo {file_path}: {e}")

    if not dataframes:
        logger.error("No se pudieron procesar los archivos.")
        return None

    # Asegurar que todos los DataFrames tengan las mismas columnas antes de combinar
    unified_dfs = unify_schemas(dataframes)

    # Combinar filas basadas en la clave 'name' y resolver duplicados
    logger.info("Consolidando filas con el mismo 'name'.")
    from pyspark.sql import functions as F

    # Renombrar las columnas para evitar ambigüedad
    for i, df in enumerate(unified_dfs):
        unified_dfs[i] = df.select(
            *[col(c).alias(f"{c}_df{i}") for c in df.columns if c != "name"],
            col("name")
        )

    # Realizar la unión por 'name'
    combined_df = unified_dfs[0]
    for df in unified_dfs[1:]:
        combined_df = combined_df.join(df, on="name", how="outer")

    # Resolver ambigüedad consolidando valores no nulos en columnas duplicadas
    final_columns = list(set(c.split("_df")[0] for c in combined_df.columns if "_df" in c or c != "name"))
    consolidated_df = combined_df.select(
        *[F.coalesce(*[col(f"{col_name}_df{i}") for i in range(len(unified_dfs)) if f"{col_name}_df{i}" in combined_df.columns]).alias(col_name)
          for col_name in final_columns],
        col("name")
    )

    logger.info("Fusión de datos completada.")
    consolidated_df.show(5, truncate=False)
    return consolidated_df




def process_generaldata_folder(subfolder):
    """Procesa carpetas dentro de GeneralData/."""
    folder = f'GeneralData/{subfolder}/'
    logger.info(f"Procesando carpeta: {folder}")

    delete_existing_folder(dest_bucket, folder)

    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=folder)
    files = [
        f"s3://{source_bucket}/{obj['Key']}"
        for obj in response.get('Contents', [])
        if obj['Key'].endswith(('.csv', '.json'))
    ]

    logger.info(f"Archivos encontrados para {subfolder}: {files}")

    if not files:
        logger.warning(f"No se encontraron archivos en {folder}")
        return

    merged_df = merge_team_data(files)
    if merged_df is None or merged_df.rdd.isEmpty():
        logger.warning(f"El DataFrame fusionado para {subfolder} está vacío. No se generará el archivo Parquet.")
        return

    if subfolder in schemas:
        merged_df = align_to_schema(merged_df, schemas[subfolder])

    dest_path = f's3://{dest_bucket}/{folder}'
    try:
        merged_df.write.mode("overwrite").parquet(dest_path)
        logger.info(f"Datos guardados correctamente en: {dest_path}")
    except Exception as e:
        logger.error(f"Error al escribir datos en {dest_path}: {e}")

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

def process_countries_folder():
    """Procesa únicamente los archivos dentro de la carpeta 'COUNTRIES' y los guarda como un solo archivo Parquet."""
    folder = 'GeneralData/COUNTRIES/'
    logger.info(f"Procesando carpeta: {folder}")

    # Eliminar la carpeta existente en el destino
    dest_folder = f"{folder}"
    delete_existing_folder(dest_bucket, dest_folder)

    # Listar archivos en la carpeta fuente
    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=folder)
    files = [f"s3://{source_bucket}/{obj['Key']}" for obj in response.get('Contents', []) if obj['Key'].endswith(('.csv', '.json'))]

    if not files:
        logger.warning(f"No se encontraron archivos en {folder}")
        return

    # Crear una lista para almacenar los DataFrames
    dataframes = []

    for file_path in files:
        try:
            # Procesar CSV
            if file_path.endswith(".csv"):
                logger.info(f"Leyendo archivo CSV: {file_path}")
                df = spark.read.option("header", "true").csv(file_path)
            # Procesar JSON
            elif file_path.endswith(".json"):
                logger.info(f"Leyendo archivo JSON: {file_path}")
                df = spark.read.option("multiline", "true").json(file_path)
            else:
                logger.warning(f"Formato no soportado: {file_path}")
                continue

            # Añadir el DataFrame a la lista
            dataframes.append(df)

        except Exception as e:
            logger.error(f"Error procesando archivo {file_path}: {e}")

    if not dataframes:
        logger.warning(f"No se pudieron procesar archivos en {folder}")
        return

    try:
        # Combinar todos los DataFrames en uno solo
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.unionByName(df)

        # Guardar en formato Parquet con un archivo único
        dest_path = f"s3://{dest_bucket}/{folder}countries.parquet"
        combined_df.coalesce(1).write.mode("overwrite").parquet(dest_path)
        logger.info(f"Archivo guardado como Parquet en: {dest_path}")

    except Exception as e:
        logger.error(f"Error al guardar el archivo Parquet para la carpeta COUNTRIES: {e}")



def main():
    process_generaldata_folder('TEAMS')
    #process_generaldata_folder('COUNTRIES')
    process_generaldata_folder('PLAYERS')
    process_countries_folder()

if __name__ == "__main__":
    main()

job.commit()
