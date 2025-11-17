import json
import os
import uuid
import logging
from datetime import datetime
from decimal import Decimal

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DYNAMO_TABLE = os.environ.get("DYNAMODB_TABLE", "SismosIGP")

def to_dynamo_value(value):
    """
    Convierte valores para que Dynamo los acepte:
    - None -> se ignora
    - int/float -> Decimal
    - otros -> tal cual (string, etc.)
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    return value

def clean_item(d):
    """
    Quita las claves con valor None y convierte números.
    """
    out = {}
    for k, v in d.items():
        v_conv = to_dynamo_value(v)
        if v_conv is not None:
            out[k] = v_conv
    return out

def lambda_handler(event, context):
    logger.info("Iniciando lambda_handler")

    api_url = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"
    params = {
        "where": "1=1",
        "outFields": "*",
        "orderByFields": "fecha DESC",
        "resultRecordCount": 10,
        "f": "json"
    }

    try:
        # 1) Llamar al API tal cual tu curl
        resp = requests.get(api_url, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        if "features" not in data:
            logger.error("La respuesta de la API no tiene 'features'")
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Respuesta inválida del API del IGP"})
            }

        features = data["features"]
        logger.info(f"Se recibieron {len(features)} features del API")

        # 2) Preparar DynamoDB
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(DYNAMO_TABLE)

        inserted = []

        # 3) Guardar cada sismo en Dynamo
        with table.batch_writer() as batch:
            for idx, feature in enumerate(features, start=1):
                attrs = feature.get("attributes", {}) or {}
                geom = feature.get("geometry", {}) or {}

                # Base del item
                item = {
                    "id": str(uuid.uuid4()),
                    "numero": idx,
                    "timestamp_scraping": datetime.utcnow().isoformat(),
                }

                # Agregar atributos del API (fecha, hora, magnitud, etc.)
                for k, v in attrs.items():
                    item[k] = v

                # Si quieres, también guardamos la geometría
                for k, v in geom.items():
                    item[f"geom_{k}"] = v

                # Limpiar None y convertir números a Decimal
                item = clean_item(item)

                batch.put_item(Item=item)
                inserted.append(item)

        logger.info(f"Se insertaron {len(inserted)} sismos en DynamoDB")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Se almacenaron {len(inserted)} sismos en DynamoDB",
                "count": len(inserted)
            })
        }

    except Exception as e:
        logger.error(f"Error en lambda_handler: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Error interno: {str(e)}"})
        }
