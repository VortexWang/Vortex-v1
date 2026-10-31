#!/usr/bin/env bash
# 按 Goose 文件格式拆分 Up / Down 执行全部迁移，并运行约束测试。连接参数取自 PG* 环境变量。
set -euo pipefail
cd "$(dirname "$0")/../backend/migrations"
PS="psql -v ON_ERROR_STOP=1 -q"
up()   { awk '/^-- \+goose Up/{f=1;next}/^-- \+goose Down/{f=0}f' "$1" | grep -v '^-- +goose' | $PS; }
down() { awk '/^-- \+goose Down/{f=1;next}f' "$1" | grep -v '^-- +goose' | $PS; }
files=$(ls 0*.sql | sort)
for f in $files; do up "$f"; echo "up   $f"; done
for t in tests/*_test.sql; do
  out=$($PS -f "$t" 2>&1) || { echo "$out"; exit 1; }
  echo "pass $t ($(grep -c 'ok   ' <<<"$out") checks)"
done
for f in $(ls 0*.sql | sort -r); do down "$f"; echo "down $f"; done
test "$($PS -Atc "select count(*) from pg_tables where schemaname='public'")" = 0
for f in $files; do up "$f"; done; echo "up again ok"
