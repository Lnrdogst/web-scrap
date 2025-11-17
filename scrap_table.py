import json
import requests
import boto3
import uuid
import os
from datetime import datetime
from decimal import Decimal

# Helper para serializar Decimal a JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

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

        # Limpiar tabla (con límite para evitar timeout)
        scan = table.scan(Limit=100)
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
                "lat": Decimal(str(attrs.get("lat"))) if attrs.get("lat") is not None else None,
                "lon": Decimal(str(attrs.get("lon"))) if attrs.get("lon") is not None else None,
                "magnitud": Decimal(str(attrs.get("magnitud"))) if attrs.get("magnitud") is not None else None,
                "profundidad": Decimal(str(attrs.get("profundidad"))) if attrs.get("profundidad") is not None else None,
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
            }, ensure_ascii=False, cls=DecimalEncoder)
        }

    except requests.exceptions.Timeout:
        return {
            'statusCode': 504,
            'body': json.dumps({"error": "Timeout al consultar API IGP"})
        }
    except requests.exceptions.RequestException as e:
        return {
            'statusCode': 502,
            'body': json.dumps({"error": f"Error en API externa: {str(e)}"})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({"error": str(e)})
        }
