import sys
import re
import boto3
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuración del bucket y patrón de carpetas
source_bucket = "uefa-building"
folder_pattern = r"^\d{4}-\d{2}$"  # Formato yyyy-yy+1

# Diccionario para normalizar nombres de equipos
synonyms = {
    "FC Barcelona": "Barcelona",
    "Barça": "Barcelona",
    "Manchester Utd": "Manchester United",
    "Man U": "Manchester United",
    "Real Madrid CF": "Real Madrid",
    "Los Blancos": "Real Madrid",
    "Bayern München": "Bayern Munich",
    "FC Bayern": "Bayern Munich",
    "Juventus FC": "Juventus",
    "La Vecchia Signora": "Juventus",
    "Paris Saint-Germain": "PSG",
    "PSG": "Paris Saint-Germain",
    "Chelsea FC": "Chelsea",
    "The Blues": "Chelsea",
    "Liverpool FC": "Liverpool",
    "The Reds": "Liverpool",
    "Manchester City FC": "Manchester City",
    "Man City": "Manchester City",
    "Inter Milano": "Inter Milan",
    "Inter Milan": "Inter Milan",
    "Atletico Madrid": "Atlético Madrid",
    "Atletico": "Atlético Madrid",
    "AC Milan": "Milan",
    "Milan AC": "Milan",
    "Tottenham Hotspur": "Tottenham",
    "Spurs": "Tottenham",
    "Sevilla FC": "Sevilla",
    "Benfica Lisbon": "Benfica",
    "SL Benfica": "Benfica",
    "Borussia Dortmund": "Dortmund",
    "BVB": "Dortmund",
    "Olympique de Marseille": "Marseille",
    "OM": "Marseille",
    "AS Roma": "Roma",
    "Giallorossi": "Roma",
    "Shakhtar Donetsk": "Shakhtar",
    "FC Shakhtar": "Shakhtar",
    "Ajax Amsterdam": "Ajax",
    "AFC Ajax": "Ajax",
    "RB Leipzig": "Leipzig",
    "RasenBallsport Leipzig": "Leipzig",
    "Zenit St. Petersburg": "Zenit",
    "FC Zenit": "Zenit",
    "Lazio Roma": "Lazio",
    "SS Lazio": "Lazio",
    "FC Porto": "Porto",
    "The Dragons": "Porto",
    "Sporting CP": "Sporting",
    "Sporting Lisbon": "Sporting",
    "Galatasaray SK": "Galatasaray",
    "Gala": "Galatasaray",
    "Red Star Belgrade": "Crvena Zvezda",
    "FK Crvena Zvezda": "Crvena Zvezda",
    "Celtic FC": "Celtic",
    "The Hoops": "Celtic"
}

# Diccionario para almacenar equipo y país
team_country_dict = {}

# Función auxiliar para buscar el país en el diccionario
def get_country(team_name):
    return team_country_dict.get(team_name, "NA")

# Registrar la función como UDF en PySpark
get_country_udf = udf(get_country, StringType())

# Obtener todas las carpetas en el bucket que cumplen con el patrón de fecha yyyy-yy+1
response = boto3.client('s3').list_objects_v2(Bucket=source_bucket, Prefix="", Delimiter='/')
folders = [content.get('Prefix') for content in response.get('CommonPrefixes', [])]

for folder in folders:
    folder_name = folder.strip('/')

    # Saltar la carpeta "2023-24"
    if folder_name == "2023-24":
        print(f"Skipping folder: {folder_name}")
        continue

    if re.match(folder_pattern, folder_name):
        print(f"Processing folder: {folder_name}")
        cleaned_path = f"{folder_name}/cleaned/"

        # Leer datos de origen
        datasource = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [f"s3://{source_bucket}/{cleaned_path}"]},
            format="parquet"
        )
        
        # Convertir a DataFrame para aplicar transformaciones
        df = datasource.toDF()

        # Mostrar columnas que están siendo revisadas
        print("Columns currently under review:", df.columns)

        # Aplicar sinónimos de nombres a nivel de código para la columna `team_name_1`
        for key, value in synonyms.items():
            df = df.withColumn(
                "team_name_1",
                when(col("team_name_1") == key, lit(value)).otherwise(col("team_name_1"))
            )

        # Construir el diccionario con equipos y países
        team_country_dict.update(
            {row["team_name_1"]: row["country_1"] for row in df.select("team_name_1", "country_1").distinct().collect() if row["country_1"] is not None}
        )

        # Completar `country_1` usando el diccionario `team_country_dict` para equipos sin país
        df = df.withColumn(
            "country_1",
            when((col("country_1").isNull()) | (col("country_1") == "NA"), get_country_udf(col("team_name_1")))
            .otherwise(col("country_1"))
        )

job.commit()


