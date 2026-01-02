#!/bin/bash
set -e

# Создание БД и таблицы
#psql -U validator -d postgres -c "CREATE DATABASE \"project-sem-1\";" 2>/dev/null || true
#psql -U validator -d "project-sem-1" -c "CREATE TABLE IF NOT EXISTS prices (id TEXT, created_at DATE, name TEXT, category TEXT, price INTEGER);"

psql 'postgresql://validator:val1dat0r@localhost:5432/postgres' -c "CREATE DATABASE \"project-sem-1\";" 2>/dev/null || true
psql 'postgresql://validator:val1dat0r@localhost:5432/project-sem-1' -c "CREATE TABLE IF NOT EXISTS prices (id TEXT, created_at DATE, name TEXT, category TEXT, price INTEGER);"
