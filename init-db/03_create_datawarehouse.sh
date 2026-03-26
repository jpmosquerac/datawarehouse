#!/bin/bash
# Crea la base de datos del almacén de datos y aplica el schema DDL.
# Este script es ejecutado por PostgreSQL durante la inicialización del contenedor,
# después de que metadata_repository ya fue creada (por POSTGRES_DB env var).
set -e

echo ">>> Creando base de datos: datawarehouse"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE datawarehouse;
EOSQL

echo ">>> Aplicando schema del almacén (dw)..."
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d datawarehouse \
    -f /docker-initdb-sql/datawarehouse.sql

echo ">>> datawarehouse inicializado correctamente."
