import json
from unittest.mock import patch
from handler import handle_sqs_message


def _make_event(payload: dict) -> dict:
    envelope = {"payload_json": json.dumps(payload)}
    return {"Records": [{"body": json.dumps(envelope), "messageId": "msg-1"}]}


@patch("handler._procesar_interaccion")
def test_procesa_un_mensaje(mock_proc):
    event = _make_event(
        {"interaccion_id": "i-1", "estudiante_id": "e-1", "curso_id": "c-1"}
    )
    result = handle_sqs_message(event, None)
    assert result["procesados"] == 1
    assert result["errores"] == 0
    mock_proc.assert_called_once()


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
                    "payload_json": json.dumps(
                        {"estudiante_id": f"e-{i}", "curso_id": "c-1"}
                    )
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
