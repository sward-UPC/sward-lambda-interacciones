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
            envelope = body
            payload = json.loads(body.get("payload_json", body.get("Message", "{}")))
            event_id = _extraer_event_id(envelope, payload)
            _procesar_interaccion(payload, event_id)
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


def _extraer_event_id(envelope: dict, payload: dict) -> str:
    """Obtiene el event_id del DomainEvent/EventEnvelope de sward-shared.

    Busca primero en el envelope (EventEnvelope) y luego en el payload
    (DomainEvent). Si no viaja en ningún sitio, genera uno determinístico a
    partir del contenido para no romper el procesamiento.
    """
    event_id = envelope.get("event_id") or payload.get("event_id")
    if event_id:
        return str(event_id)

    logger.warning(
        "Evento sin event_id, se genera uno determinístico | payload=%s", payload
    )
    base = json.dumps(payload, sort_keys=True, default=str)
    return str(uuid.uuid5(uuid.NAMESPACE_URL, base))


def _procesar_interaccion(payload: dict, event_id: str) -> None:
    from lib.db_client import get_connection
    from lib.idempotency import ya_procesado

    estudiante_id = payload.get("estudiante_id")
    curso_id = payload.get("curso_id")
    occurred_at = payload.get("occurred_at", datetime.now(timezone.utc).isoformat())

    if not estudiante_id or not curso_id:
        logger.warning("Payload incompleto, se omite | payload=%s", payload)
        return

    # processed_events vive en trazabilidad_db (misma conexión que el negocio),
    # por lo que el dedup y el UPDATE/INSERT son atómicos en una sola transacción.
    with get_connection() as conn:
        if ya_procesado(conn, event_id):
            conn.commit()
            logger.info(
                "Evento duplicado, omitido | event_id=%s | estudiante=%s | curso=%s",
                event_id,
                estudiante_id,
                curso_id,
            )
            return

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
        "Interacción procesada | event_id=%s | estudiante=%s | curso=%s",
        event_id,
        estudiante_id,
        curso_id,
    )


lambda_handler = handle_sqs_message
