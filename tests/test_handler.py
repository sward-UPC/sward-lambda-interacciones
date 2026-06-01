"""Tests unitarios para sward-lambda-interacciones.

Trigger: SQS → normaliza InteraccionRegistradaEvent
"""
import json
from unittest.mock import MagicMock, patch


from handler import handle_sqs_message


def _make_event(payload: dict, event_id: str | None = None) -> dict:
    envelope = {"payload_json": json.dumps(payload)}
    if event_id is not None:
        envelope["event_id"] = event_id
    return {"Records": [{"body": json.dumps(envelope), "messageId": "msg-1"}]}


@patch("handler._procesar_interaccion")
def test_procesa_un_mensaje(mock_proc):
    event = _make_event(
        {"interaccion_id": "i-1", "estudiante_id": "e-1", "curso_id": "c-1"},
        event_id="evt-1",
    )
    result = handle_sqs_message(event, None)
    assert result["procesados"] == 1
    assert result["errores"] == 0
    mock_proc.assert_called_once()
    # El handler debe pasar el event_id extraído a la lógica de negocio.
    assert mock_proc.call_args[0][1] == "evt-1"


@patch("handler._procesar_interaccion")
def test_cuenta_errores(mock_proc):
    mock_proc.side_effect = RuntimeError("db error")
    event = _make_event({"estudiante_id": "e-1", "curso_id": "c-1"})
    result = handle_sqs_message(event, None)
    assert result["errores"] == 1
    assert result["procesados"] == 0


@patch("handler._procesar_interaccion")
def test_procesa_multiples_mensajes(mock_proc):
    records = [
        {
            "body": json.dumps(
                {
                    "event_id": f"evt-{i}",
                    "payload_json": json.dumps(
                        {"estudiante_id": f"e-{i}", "curso_id": "c-1"}
                    ),
                }
            ),
            "messageId": f"msg-{i}",
        }
        for i in range(3)
    ]
    result = handle_sqs_message({"Records": records}, None)
    assert result["procesados"] == 3
    assert mock_proc.call_count == 3


def test_evento_sin_records():
    result = handle_sqs_message({}, None)
    assert result["procesados"] == 0
    assert result["errores"] == 0


# ---------------------------------------------------------------------------
# Idempotencia (deduplicación por event_id)
# ---------------------------------------------------------------------------


def _conn_con_rowcount(rowcount: int) -> MagicMock:
    """Construye un mock de conexión psycopg2 cuyo cursor reporta rowcount."""
    cur = MagicMock()
    cur.rowcount = rowcount
    conn = MagicMock()
    # El cursor se usa como context manager: conn.cursor().__enter__() -> cur
    conn.cursor.return_value.__enter__.return_value = cur
    return conn, cur


def test_primer_evento_se_procesa():
    """rowcount=1 en el INSERT del dedup => ya_procesado=False => se procesa."""
    from lib.idempotency import ya_procesado

    conn, _ = _conn_con_rowcount(1)
    assert ya_procesado(conn, "evt-1") is False


def test_evento_repetido_se_omite():
    """rowcount=0 (ON CONFLICT DO NOTHING) => ya_procesado=True => se omite."""
    from lib.idempotency import ya_procesado

    conn, _ = _conn_con_rowcount(0)
    assert ya_procesado(conn, "evt-1") is True


def test_negocio_no_se_ejecuta_en_duplicado():
    """Para un event_id ya visto, el UPDATE/INSERT de negocio no debe correr."""
    import handler

    # Primer evento: rowcount=1 (insertado) -> se ejecuta el negocio.
    # Segundo evento: rowcount=0 (conflicto) -> negocio omitido.
    conn1, cur1 = _conn_con_rowcount(1)
    conn2, cur2 = _conn_con_rowcount(0)
    conns = iter([conn1, conn2])

    def _fake_get_connection():
        from contextlib import contextmanager

        @contextmanager
        def _cm():
            yield next(conns)

        return _cm()

    payload = {"estudiante_id": "e-1", "curso_id": "c-1"}
    with patch("lib.db_client.get_connection", side_effect=_fake_get_connection):
        handler._procesar_interaccion(payload, "evt-dup")
        handler._procesar_interaccion(payload, "evt-dup")

    # En el primer evento (rowcount=1) el cursor ejecuta dedup + UPDATE (>=2 execs).
    assert cur1.execute.call_count >= 2
    # En el duplicado (rowcount=0) solo corre el dedup dentro de ya_procesado;
    # no se ejecuta ninguna sentencia de negocio adicional sobre ese cursor.
    assert cur2.execute.call_count == 2  # CREATE TABLE + INSERT del dedup


def test_event_id_ausente_genera_deterministico():
    """Sin event_id, se genera uno determinístico y no rompe el procesamiento."""
    from handler import _extraer_event_id

    payload = {"estudiante_id": "e-1", "curso_id": "c-1"}
    a = _extraer_event_id({}, payload)
    b = _extraer_event_id({}, payload)
    assert a == b  # determinístico
    assert a


def test_event_id_en_envelope_tiene_prioridad():
    from handler import _extraer_event_id

    assert _extraer_event_id({"event_id": "env-1"}, {"event_id": "pay-1"}) == "env-1"
    assert _extraer_event_id({}, {"event_id": "pay-1"}) == "pay-1"
