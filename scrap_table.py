import json
import requests
import boto3
import uuid
import os
import logging
from datetime import datetime
from decimal import Decimal

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Helper para serializar Decimal a JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def lambda_handler(event, context):
    logger.info("Iniciando lambda_handler")
    
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
        logger.info(f"Consultando API: {api_url}")
        response = requests.get(api_url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        logger.info(f"Respuesta recibida de API, status: {response.status_code}")

        if "features" not in data:
            logger.error("No se encontró 'features' en la respuesta de la API")
            return {
                'statusCode': 500,
                'body': json.dumps({"error": "No se encontraron datos en API IGP"})
            }

        features = data["features"]
        logger.info(f"Se encontraron {len(features)} sismos en la API")

        # Inicializar DynamoDB
        dynamodb = boto3.resource('dynamodb')
        table_name = os.environ.get('DYNAMODB_TABLE', 'SismosIGP')
        logger.info(f"Conectando a DynamoDB tabla: {table_name}")
        table = dynamodb.Table(table_name)

        # Limpiar tabla (con límite para evitar timeout)
        logger.info("Limpiando registros antiguos de DynamoDB")
        scan = table.scan(Limit=100)
        with table.batch_writer() as batch:
            for item in scan.get("Items", []):
                batch.delete_item(Key={'id': item['id']})
        logger.info(f"Se eliminaron {len(scan.get('Items', []))} registros antiguos")

        # Insertar nuevos datos
        inserted = []
        for idx, feature in enumerate(features, 1):
            try:
                attrs = feature.get("attributes", {})
                
                # Validar y convertir valores numéricos de forma segura
                def safe_decimal(value):
                    if value is None or value == "":
                        return None
                    try:
                        return Decimal(str(value))
                    except (ValueError, TypeError) as e:
                        logger.warning(f"No se pudo convertir valor a Decimal: {value}, error: {e}")
                        return None

                item = {
                    "id": str(uuid.uuid4()),
                    "numero": idx,
                    "timestamp_scraping": datetime.utcnow().isoformat(),
                    "fecha": attrs.get("fecha"),
                    "hora": attrs.get("hora"),
                    "lat": safe_decimal(attrs.get("lat")),
                    "lon": safe_decimal(attrs.get("lon")),
                    "magnitud": safe_decimal(attrs.get("magnitud")),
                    "profundidad": safe_decimal(attrs.get("profundidad")),
                    "departamento": attrs.get("departamento"),
                    "ref": attrs.get("ref"),
                }

                # Filtrar valores None antes de insertar
                item = {k: v for k, v in item.items() if v is not None}
                
                table.put_item(Item=item)
                inserted.append(item)
                logger.info(f"Sismo {idx} insertado correctamente")
                
            except Exception as item_error:
                logger.error(f"Error procesando sismo {idx}: {str(item_error)}")
                # Continuar con los demás sismos
                continue

        logger.info(f"Proceso completado. Total insertados: {len(inserted)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                "message": f"Se almacenaron {len(inserted)} sismos",
                "sismos": inserted
            }, ensure_ascii=False, cls=DecimalEncoder)
        }

    except requests.exceptions.Timeout:
        logger.error("Timeout al consultar API IGP")
        return {
            'statusCode': 504,
            'body': json.dumps({"error": "Timeout al consultar API IGP"})
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Error en petición a API: {str(e)}")
        return {
            'statusCode': 502,
            'body': json.dumps({"error": f"Error en API externa: {str(e)}"})
        }
    except Exception as e:
        logger.error(f"Error general no manejado: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({"error": f"Error interno: {str(e)}"})
        }
