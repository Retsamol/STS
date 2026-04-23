# DBGIT

Пакет для переноса и разворачивания PostgreSQL с заполненными схемами `inventory_v2` и `topo`.

Приложена инструкция ## DB_GUIDE_RU.md

## Состав

- `infra/postgres/docker-compose.yml`
- `infra/postgres/.env.example`
- `db/postgres/schema.sql`
- `db/postgres/schema_v2.sql`
- `db/exports/inventory_v2_topo_2026-04-22_22-29-34.dump`
- `restore_db.ps1`

## Быстрое восстановление

1. Перейдите в папку (через консоль) в которую всё экспортируете.
2. Убедитесь, что установлен Docker Desktop.
3. Запустите:

```powershell
powershell -ExecutionPolicy Bypass -File .\restore_db.ps1
```

Скрипт:

- поднимет контейнер PostgreSQL;
- дождется готовности БД;
- скопирует дамп в контейнер;
- восстановит данные в базу `topo`.

После восстановления можно подключаться к базе:

- host: `127.0.0.1`
- port: `5432`
- database: `topo`
- user: `postgres`
- password: `postgres`

## SQL и Python

Пример SQL:

```sql
select count(*) from inventory_v2.satellite;
select count(*) from topo.edge;
```

Пример SQLAlchemy:

```python
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg://postgres:postgres@127.0.0.1:5432/topo")
with engine.connect() as conn:
    satellites = conn.execute(text("select count(*) from inventory_v2.satellite")).scalar()
    print(satellites)
```
