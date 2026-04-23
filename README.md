# DBGIT

Пакет для переноса и разворачивания PostgreSQL с заполненными схемами `inventory_v2` и `topo`.

## Состав

- `infra/postgres/docker-compose.yml`
- `infra/postgres/.env.example`
- `db/postgres/schema.sql`
- `db/postgres/schema_v2.sql`
- `db/exports/inventory_v2_topo_2026-04-22_22-29-34.dump`
- `restore_db.ps1`
- `DB_GUIDE_RU.md`

## Что важно знать заранее

Текущий файл `inventory_v2_topo_2026-04-22_22-29-34.dump` — это корректный дамп

- восстанавливать его нужно через `pg_restore`;

## Быстрое восстановление

1. Перейдите в папку.
2. Убедитесь, что установлен Docker Desktop.
3. Если репозиторий получен через `Git`, обязательно подтяните реальные файлы `Git LFS`:

```powershell
git lfs install
git lfs pull
```

4. Проверьте размер файла `db/exports/inventory_v2_topo_2026-04-22_22-29-34.dump`.

Он должен быть большим, примерно `273764235` байт. Если файл весит несколько сотен байт или несколько килобайт, это не дамп, а указатель `Git LFS`.

5. Запустите:

```powershell
powershell -ExecutionPolicy Bypass -File .\restore_db.ps1
```

Скрипт:

- поднимет контейнер PostgreSQL;
- дождется готовности базы;
- скопирует дамп в контейнер;
- восстановит данные в базу `topo`.

После восстановления можно подключаться к базе:

- host: `127.0.0.1`
- port: `5432`
- database: `topo`
- user: `postgres`
- password: `postgres`

## Если у другого человека ошибка `dump too short`

Это почти всегда означает, что он получил не настоящий файл дампа, а указатель `Git LFS`.

Обычные причины:

- репозиторий клонировали без `Git LFS`;
- скачали архив репозитория вместо нормального клона;
- скачали не сам объект, а текстовый файл-указатель.

Что делать:

```powershell
git lfs install
git lfs pull
```

Потом снова проверить размер файла и только после этого запускать восстановление.

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

## Как залить на GitHub

Файл дампа больше лимита обычного GitHub-файла, поэтому загружать его нужно через Git LFS. Через веб-интерфейс GitHub этот `.dump` не загрузится.

Если репозиторий приватный, это нормально.

Команды:

```powershell
git lfs install
git add DBGIT\.gitattributes
git add DBGIT
git commit -m "Add PostgreSQL snapshot package"
git push origin <branch-name>
```

Если `git-lfs` еще не установлен, установите его сначала. Альтернатива: хранить `.dump` не в `git`-истории, а как `GitHub Release asset`.
