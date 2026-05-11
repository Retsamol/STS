# Hybrid Topology Results

Я собрал здесь отдельный пакет с результатами расчетов по построенным топологиям. Внутри лежит только схема `topo`: сами прогоны, узлы, координаты, связи, лучи, traffic results, очереди, распределение ресурсов, результаты радиобюджета и готовые JSON-снимки топологии.

Этот пакет нужен для просмотра и анализа уже построенных топологий. Состав папки рассчитан на простое восстановление результатов в локальный PostgreSQL и работу с ними через SQL-клиент или собственные аналитические скрипты.

Подробное описание того, какие таблицы читать и с чего начинать, находится в `DB_GUIDE_RU.md`.

## Состав пакета

В папке `db` лежат SQL-структура текущей схемы `topo` и дамп с данными:

```text
db/topo_results_schema_2026-05-11.sql
db/exports/topo_results_2026-05-11.dump
```

В папке `infra/postgres` лежит минимальный Docker Compose для локального PostgreSQL. Скрипт `restore_db.ps1` поднимает контейнер и восстанавливает дамп или, если нужно, создает пустую структуру `topo` без данных.

Файл дампа большой: `1570929648` байт, примерно 1.57 ГБ. Он должен храниться через Git LFS. Если после скачивания `.dump` весит несколько сотен байт или пару килобайт, значит Git LFS не подтянул настоящий файл.

```powershell
git lfs install
git lfs pull
```

## Восстановление

Перед запуском нужен установленный и запущенный Docker Desktop. После клонирования репозитория сначала проверьте Git LFS, затем размер дампа:

```powershell
git lfs install
git lfs pull
Get-Item .\db\exports\topo_results_2026-05-11.dump
```

Для восстановления данных выполните:

```powershell
powershell -ExecutionPolicy Bypass -File .\restore_db.ps1 -RestoreDump
```

Если нужно пересоздать локальный Docker volume PostgreSQL с нуля:

```powershell
powershell -ExecutionPolicy Bypass -File .\restore_db.ps1 -ResetVolume -RestoreDump
```

После восстановления база доступна с такими параметрами:

```text
host: 127.0.0.1
port: 5432
database: topo
user: postgres
password: postgres
```

Если нужны только таблицы без данных, можно запустить скрипт без `-RestoreDump`:

```powershell
powershell -ExecutionPolicy Bypass -File .\restore_db.ps1
```

## Что смотреть в базе

Начинать удобнее с `topo.simulation`: там лежит список прогонов и их `simulation_uuid`. После выбора нужного `simulation_uuid` уже можно смотреть узлы в `topo.node`, координаты в `topo.node_position`, связи в `topo.edge`, лучи в `topo.ray` и `topo.connected_ray`, готовые кадры в `topo.topology_snapshot`.

Для результатов по трафику используются `topo.traffic_flow_result`, `topo.traffic_resource_allocation` и `topo.node_queue_state`. Для радиобюджета используется `topo.link_budget_result`.

## Загрузка в GitHub

Дамп больше обычных лимитов GitHub, поэтому для него нужен Git LFS:

```powershell
git lfs install
git add .gitattributes
git add db infra README.md DB_GUIDE_RU.md restore_db.ps1
git commit -m "Add topo results dump"
git push
```

Если LFS-лимиты в репозитории окажутся неудобными, сам `.dump` лучше положить в GitHub Release, а в репозитории оставить схему, скрипт восстановления и документацию.
