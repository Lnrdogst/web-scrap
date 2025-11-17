import json
import requests
import boto3
import uuid
import os
from datetime import datetime

def lambda_handler(event, context):
    # API real donde están los datos
    api_url = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"

    params = {
        "where": "1=1",
        "outFields": "*",
        "orderByFields": "fecha DESC",
        "resultRecordCount": 10,
        "f": "json"
    }

    try:
        response = requests.get(api_url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()

        if "features" not in data:
            return {
                'statusCode': 500,
                'body': json.dumps({"error": "No se encontraron datos en API IGP"})
            }

        features = data["features"]

        # Inicializar DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table_name = os.environ.get('DYNAMODB_TABLE', 'SismosIGP')
        table = dynamodb.Table(table_name)

        # Limpiar tabla
        scan = table.scan()
        with table.batch_writer() as batch:
            for item in scan.get("Items", []):
                batch.delete_item(Key={'id': item['id']})

        # Insertar nuevos datos
        inserted = []
        for idx, feature in enumerate(features, 1):
            attrs = feature["attributes"]

            item = {
                "id": str(uuid.uuid4()),
                "numero": idx,
                "timestamp_scraping": datetime.utcnow().isoformat(),
                "fecha": attrs.get("fecha"),
                "hora": attrs.get("hora"),
                "lat": attrs.get("lat"),
                "lon": attrs.get("lon"),
                "magnitud": attrs.get("magnitud"),
                "profundidad": attrs.get("profundidad"),
                "departamento": attrs.get("departamento"),
                "ref": attrs.get("ref"),
            }

            table.put_item(Item=item)
            inserted.append(item)

        return {
            'statusCode': 200,
            'body': json.dumps({
                "message": f"Se almacenaron {len(inserted)} sismos",
                "sismos": inserted
            }, ensure_ascii=False)
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }
