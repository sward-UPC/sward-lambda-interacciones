import json
import logging
import uuid
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handle_sqs_message(event: dict, context) -> dict:
    """Trigger: SQS — procesa InteraccionRegistradaEvent y persiste en trazabilidad_db."""
    records = event.get("Records", [])
    procesados = 0
    errores = 0

    for record in records:
        try:
            body = json.loads(record["body"])
            payload = json.loads(body.get("payload_json", body.get("Message", "{}")))
            _procesar_interaccion(payload)
            procesados += 1
        except Exception as e:
            logger.error(
                "Error procesando mensaje SQS | error=%s | record=%s",
                e,
                record.get("messageId"),
            )
            errores += 1

    logger.info(
        "Procesamiento completado | procesados=%d | errores=%d", procesados, errores
    )
    return {"procesados": procesados, "errores": errores}


def _procesar_interaccion(payload: dict) -> None:
    from lib.db_client import get_connection

    estudiante_id = payload.get("estudiante_id")
    curso_id = payload.get("curso_id")
    occurred_at = payload.get("occurred_at", datetime.now(timezone.utc).isoformat())

    if not estudiante_id or not curso_id:
        logger.warning("Payload incompleto, se omite | payload=%s", payload)
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE academic_progress
                SET total_interacciones = total_interacciones + 1,
                    ultima_actividad = %s
                WHERE estudiante_id = %s AND curso_id = %s
            """,
                (occurred_at, estudiante_id, curso_id),
            )

            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO academic_progress
                        (id, estudiante_id, curso_id, porcentaje_avance, nivel_riesgo,
                         total_interacciones, recursos_completados, puntaje_promedio, ultima_actividad)
                    VALUES (%s, %s, %s, 0.0, 'bajo', 1, 0, 0.0, %s)
                    ON CONFLICT (estudiante_id, curso_id) DO UPDATE
                        SET total_interacciones = academic_progress.total_interacciones + 1,
                            ultima_actividad = EXCLUDED.ultima_actividad
                """,
                    (str(uuid.uuid4()), estudiante_id, curso_id, occurred_at),
                )

        conn.commit()
    logger.info(
        "Interacción procesada | estudiante=%s | curso=%s", estudiante_id, curso_id
    )


lambda_handler = handle_sqs_message
