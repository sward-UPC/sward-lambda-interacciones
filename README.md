# sward-lambda-interacciones

AWS Lambda del sistema **SWARD** que procesa asincrónicamente las interacciones académicas de los estudiantes.

## Trigger

**Amazon SQS** — enrutado desde Amazon EventBridge cuando se publica un `InteraccionRegistradaEvent`.

## Acción

Normaliza y limpia los datos de interacción académica, luego los persiste directamente en la base de datos de trazabilidad (`trazabilidad_db`).

## Estructura

```
handler.py          # LambdaInteraccionesHandler.handle_sqs_message()
lib/
  db_client.py      # psycopg3 directo (sin ORM)
  logger.py         # Structured JSON logger para CloudWatch
requirements.txt
template.yaml       # AWS SAM template
Makefile            # make deploy | make test | make invoke
```

## Stack

- Python 3.11 · psycopg3 · boto3 · AWS SAM

## Despliegue

```bash
make deploy ENV=staging
```

## Tests

```bash
make test
```

## Proyecto

**TP202610051** — Universidad Peruana de Ciencias Aplicadas (UPC)  
Taller de Proyecto 1 / 2026
