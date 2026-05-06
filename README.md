# Hybrid Topology DB

Я собрал здесь отдельный пакет с базой для тех, кому нужно быстро развернуть у себя PostgreSQL со схемами и текущим снимком результатов расчета.

Это не весь расчетный проект. Здесь только то, что нужно для базы: SQL-схемы, миграции, docker-compose, дамп и скрипт восстановления.

Подробное описание таблиц и того, где что искать, лежит в `DB_GUIDE_RU.md`.

## Что внутри

- `infra/postgres/docker-compose.yml`
- `infra/postgres/.env.example`
- `db/postgres/schema.sql`
- `db/postgres/schema_v2.sql`
- `db/postgres/explicit_scenario_schema.sql`
- `db/postgres/migrations/*.sql`
- `db/exports/topo_current_2026-05-06.dump`
- `restore_db.ps1`
- `DB_GUIDE_RU.md`

## Самое важное заранее

Файл `db/exports/topo_current_2026-05-06.dump` - это текущий дамп базы `topo`.

Размер дампа сейчас примерно `1721737363` байт, то есть около 1.7 ГБ. Поэтому он должен храниться через Git LFS. Если после скачивания файл весит несколько сотен байт или пару килобайт, это не дамп, а LFS-указатель.

В таком случае сначала выполните:

```powershell
git lfs install
git lfs pull
```

И только потом запускайте восстановление.

## Быстрое восстановление

1. Откройте папку с этим репозиторием.
2. Убедитесь, что установлен и запущен Docker Desktop.
3. Если репозиторий получен через GitHub Desktop или обычный Git, проверьте Git LFS:

```powershell
git lfs install
git lfs pull
```

4. Проверьте размер файла:

```powershell
Get-Item .\db\exports\topo_current_2026-05-06.dump
```

5. Запустите восстановление:

```powershell
powershell -ExecutionPolicy Bypass -File .\restore_db.ps1 -RestoreDump
```

Если нужно пересоздать локальный volume PostgreSQL с нуля:

```powershell
powershell -ExecutionPolicy Bypass -File .\restore_db.ps1 -ResetVolume -RestoreDump
```

Скрипт поднимет контейнер PostgreSQL, дождется готовности базы, скопирует дамп внутрь контейнера и восстановит данные в базу `topo`.

## Подключение

После восстановления можно подключаться так:

- host: `127.0.0.1`
- port: `5432`
- database: `topo`
- user: `postgres`
- password: `postgres`

## Если нужен только пустой каркас БД

Можно не восстанавливать дамп, а просто применить схемы:

```powershell
powershell -ExecutionPolicy Bypass -File .\restore_db.ps1
```

В этом режиме создаются схемы `topo`, `topo_v2`, `inventory`, `inventory_v2` и `inventory_explicit`, но без данных из дампа.

## Если возникает `dump too short`

Почти всегда это значит, что на машине лежит не настоящий `.dump`, а Git LFS pointer.

Проверьте размер файла. Если он маленький, выполните:

```powershell
git lfs install
git lfs pull
```

Потом еще раз проверьте размер и повторите восстановление.

## Как заливать на GitHub

Дамп большой, поэтому обычная загрузка через веб-интерфейс GitHub здесь не подходит. Нужен Git LFS.

<<<<<<< HEAD
Минимальный порядок такой:

```powershell
git lfs install
git add .gitattributes
git add db infra README.md DB_GUIDE_RU.md restore_db.ps1
git commit -m "Add PostgreSQL database package"
git push
```

Если GitHub начнет ругаться на лимиты LFS, дамп лучше вынести в GitHub Release asset, а в репозитории оставить схемы, скрипт восстановления и инструкцию.
=======
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
>>>>>>> 4c20c439c247dea4c76c7e7aed9ba94e99a64a5d
