# sward-lambda-interacciones

AWS Lambda (Python) del sistema **SWARD** que consume eventos de interacción académica de
forma **asíncrona** y los persiste en la base de datos de trazabilidad.

## Qué hace

Cada vez que un estudiante interactúa con un recurso académico, el sistema publica un
`InteraccionRegistradaEvent`. Esta Lambda consume esos eventos desde una cola SQS y
actualiza el progreso académico del estudiante en la tabla `academic_progress` de
`trazabilidad_db`:

- Incrementa `total_interacciones` del par `(estudiante_id, curso_id)`.
- Actualiza `ultima_actividad` con el `occurred_at` del evento.
- Si no existe fila para ese par, la crea con valores iniciales (`UPSERT` vía
  `ON CONFLICT`).

Si el payload no trae `estudiante_id` o `curso_id`, el mensaje se omite con un warning
(no se procesa ni envenena la cola).

## Trigger

**Amazon SQS**, alimentada desde **Amazon EventBridge**. El handler tolera varias formas
del cuerpo del mensaje y extrae el payload real en este orden de precedencia:

| Forma del `body` | Origen | Dónde está el payload |
|---|---|---|
| `payload_json` | `EventEnvelope` de `sward-shared` | `json.loads(body["payload_json"])` |
| `Message` | SNS → SQS | `json.loads(body["Message"])` |
| `detail` | EventBridge → SQS (sin input transformer) | `body["detail"]` |
| cuerpo crudo | EventBridge con input transformer a `$.detail` | `body` |

## Qué persiste

Tabla `academic_progress` (en `trazabilidad_db`):

- `estudiante_id`, `curso_id` (clave de negocio, `UNIQUE`)
- `total_interacciones` (incremental)
- `ultima_actividad` (timestamp del evento)
- En el alta inicial: `porcentaje_avance=0.0`, `nivel_riesgo='bajo'`,
  `recursos_completados=0`, `puntaje_promedio=0.0`

## Idempotencia

La entrega SQS/EventBridge es **at-least-once**: un mismo evento puede reentregarse. Para
garantizar que cada evento se procese una sola vez:

- Cada evento se identifica por `event_id` (del `EventEnvelope` o del `DomainEvent`). Si no
  viaja `event_id`, se genera uno **determinístico** a partir del contenido (`uuid5`), de
  modo que reentregas del mismo payload colapsan al mismo id.
- La tabla `processed_events (event_id PRIMARY KEY)` registra los eventos ya vistos. El
  marcado se hace con `INSERT ... ON CONFLICT DO NOTHING`: si el `event_id` ya existía, el
  evento se omite.
- El **dedup y la escritura de negocio comparten la misma transacción** (una sola conexión,
  un solo `commit`), por lo que son atómicos: o se registran ambos o ninguno.

## Variables de entorno y secretos

La conexión a la BD se resuelve en este orden (`lib/db_client.py`):

1. `DATABASE_URL` — URL completa `postgresql://...` (uso local/dev).
2. Si no está, se compone desde:
   - `DATABASE_HOST`, `DATABASE_PORT` (def. `5432`), `DATABASE_NAME`
   - `DB_SECRET_ARN` — ARN de un secreto en **AWS Secrets Manager** con `{username, password}`
   - `AWS_REGION` (def. `us-east-1`)

| Variable | Requerida | Descripción |
|---|---|---|
| `DATABASE_URL` | alternativa | URL completa de conexión (tiene prioridad) |
| `DATABASE_HOST` | sí (si no hay URL) | host de `trazabilidad_db` |
| `DATABASE_PORT` | no | puerto (def. `5432`) |
| `DATABASE_NAME` | sí (si no hay URL) | nombre de la BD |
| `DB_SECRET_ARN` | sí (si no hay URL) | secreto con credenciales |
| `AWS_REGION` | no | región (def. `us-east-1`) |
| `LOG_LEVEL` | no | nivel de log (def. `INFO`) |

No hay credenciales en el código. Ver `.env.example` para el modo local.

## Build y deploy

La imagen de contenedor de la Lambda se construye y publica automáticamente a **GHCR** al
hacer push a la rama **`deploy`** (workflow `.github/workflows/build-push.yml`, que reutiliza
`sward-UPC/.github`). El despliegue de infraestructura toma esa imagen.

```bash
git push origin deploy   # dispara build & push de la imagen
```

Build local de la imagen (opcional, para probar):

```bash
docker build -t sward-lambda-interacciones .
```

La definición de la función (trigger SQS con `BatchSize: 10` y `ReportBatchItemFailures`)
está en `template.yaml` (AWS SAM).

## Testear

```bash
make test                 # instala deps de dev y corre pytest
# o directamente:
pytest -q
ruff check .              # lint
```

Los tests (`tests/test_handler.py`) cubren: procesamiento de uno y varios mensajes, conteo
de errores, evento sin records, deduplicación por `event_id` (primer evento vs. repetido) y
generación determinística de `event_id` cuando falta.

## Estructura

```
handler.py              # ciclo SQS, extracción de envelope/event_id, UPSERT de negocio
lib/
  db_client.py          # conexión psycopg2 (Secrets Manager o DATABASE_URL)
  idempotency.py        # dedup por event_id (processed_events)
  logger.py             # logger JSON para CloudWatch
tests/                  # tests unitarios (pytest)
template.yaml           # AWS SAM (trigger SQS)
Dockerfile              # imagen Lambda (python:3.11)
```

## Proyecto

**TP202610051** — Universidad Peruana de Ciencias Aplicadas (UPC) · Taller de Proyecto · 2026
