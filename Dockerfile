FROM python:3.12-slim

# Estructura interna refleja la del proyecto:
#   /app/etl/etl_dw.py  →  STG_DIR resuelve a /app/stg
WORKDIR /app/etl

COPY etl/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl/etl_dw.py .

CMD ["python", "etl_dw.py"]
