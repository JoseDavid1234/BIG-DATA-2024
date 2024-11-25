import boto3
import time
import json

def lambda_handler(event, context):
    # Cliente de Athena
    athena_client = boto3.client('athena', region_name='us-east-1')
    output_location = 's3://athena-resultados-grupo-god/resultados/'
    
    # Parámetros de la consulta
    database = 'halloweendb_raw_to_build_v1'
    query = 'SELECT * FROM countries;'
    
    # Ejecutar la consulta
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': output_location}
    )
    
    # Obtener el ID de la consulta
    query_execution_id = response['QueryExecutionId']
    
    # Verificar el estado de la consulta
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = query_status['QueryExecution']['Status']['State']
        
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)  # Esperar antes de volver a consultar el estado
    
    # Manejar resultados
    if status == 'SUCCEEDED':
        # Obtener los resultados de la consulta
        result = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        
        # Extraer los datos de las filas
        rows = result['ResultSet']['Rows']
        
        # Para retorno más limpio, extraemos los encabezados y datos
        columns = [column['VarCharValue'] for column in rows[0]['Data']]
        data = []
        
        for row in rows[1:]:
            data.append([col.get('VarCharValue', None) for col in row['Data']])
        
        # Devolver los resultados en formato JSON
        return {
            'statusCode': 200,
            'body': json.dumps({  # Convierte el body a JSON
                'columns': columns,
                'data': data
            }),
            'headers': {  # Cabeceras CORS si es necesario
                'Access-Control-Allow-Origin': '*',  # Permite acceso desde cualquier dominio
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            }
        }
    
    else:
        # Si la consulta falla
        return {
            'statusCode': 400,
            'body': json.dumps({  # Convierte el body a JSON
                'message': f"Consulta fallida con estado: {status}"
            }),
            'headers': {  # Cabeceras CORS si es necesario
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            }
        }
