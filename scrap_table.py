import json
import requests
from bs4 import BeautifulSoup
import boto3
import uuid
import os
from datetime import datetime

def lambda_handler(event, context):
    # URL de la página web con los sismos reportados por el IGP
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    try:
        # Realizar la solicitud HTTP a la página web
        response = requests.get(url, timeout=20)
        if response.status_code != 200:
            return {
                'statusCode': response.status_code,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': 'Error al acceder a la página web del IGP'})
            }

        # Parsear el contenido HTML de la página web
        soup = BeautifulSoup(response.content, 'html.parser')

        # Encontrar la tabla en el HTML
        table = soup.find('table', {'class': 'table'})
        if not table:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': 'No se encontró la tabla de sismos en la página web'})
            }

        # Extraer los encabezados de la tabla
        headers_row = table.find('thead')
        if headers_row:
            headers = [th.text.strip() for th in headers_row.find_all('th')]
        else:
            headers = []

        # Extraer las primeras 10 filas de datos de la tabla
        rows_data = []
        tbody = table.find('tbody')
        if tbody:
            rows = tbody.find_all('tr')[:10]  # Limitar a los 10 últimos sismos
            
            for row in rows:
                cells = row.find_all('td')
                if len(cells) > 0:
                    row_dict = {}
                    for i, cell in enumerate(cells):
                        if i < len(headers):
                            row_dict[headers[i]] = cell.text.strip()
                        else:
                            row_dict[f'columna_{i}'] = cell.text.strip()
                    rows_data.append(row_dict)

        if not rows_data:
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({'error': 'No se encontraron datos de sismos'})
            }

        # Guardar los datos en DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table_name = os.environ.get('DYNAMODB_TABLE', 'SismosIGP')
        dynamo_table = dynamodb.Table(table_name)

        # Limpiar la tabla antes de insertar nuevos datos
        try:
            scan = dynamo_table.scan()
            with dynamo_table.batch_writer() as batch:
                for item in scan.get('Items', []):
                    batch.delete_item(Key={'id': item['id']})
        except Exception as e:
            print(f"Error al limpiar la tabla: {str(e)}")

        # Insertar los nuevos datos
        inserted_items = []
        for idx, row in enumerate(rows_data, 1):
            item = {
                'id': str(uuid.uuid4()),
                'numero': idx,
                'timestamp_scraping': datetime.utcnow().isoformat(),
                **row
            }
            dynamo_table.put_item(Item=item)
            inserted_items.append(item)

        # Retornar el resultado como JSON
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': f'Se obtuvieron y almacenaron {len(inserted_items)} sismos del IGP',
                'sismos': inserted_items
            }, ensure_ascii=False)
        }

    except requests.exceptions.RequestException as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': f'Error en la solicitud HTTP: {str(e)}'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': f'Error inesperado: {str(e)}'})
        }
