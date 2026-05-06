# Руководство по базе `inventory_v2` и `topo`

Я оставляю здесь короткое объяснение, как читать эту базу и с чего начинать. Это не академическое описание всех полей, а практическая карта: куда смотреть, если нужно разобраться с входными данными, расчетом и результатами модели.

## 1. Что это за база

В этом пакете есть несколько схем, но основные для работы две:

- `inventory_v2` - входной слой. Здесь лежат каталоги объектов, профили оборудования, правила назначения профилей, сохраненные наборы выбора и снимки запросов на запуск.
- `topo` - выходной слой. Здесь лежат результаты конкретных расчетов: узлы, время, координаты, связи, лучи, сервисные строки по абонентам, результаты link budget и JSON-снимки топологии.

Если коротко, цепочка такая:

```text
inventory_v2 -> расчет модели -> topo
```

Старые схемы `inventory` и `topo_v2` тоже оставлены в пакете, потому что они нужны для совместимости и миграций. Но если надо понять актуальный расчет, в первую очередь смотрите `inventory_v2` и `topo`.

## 2. Что читать под разные задачи

| Задача | С чего начать |
| --- | --- |
| Посмотреть, какие наборы объектов есть в базе | `inventory_v2.selection_catalog` и таблицы `*_catalog_member` |
| Найти паспорт конкретного объекта | `inventory_v2.gateway`, `target`, `satellite`, `haps`, `vsat`, `theoretical_subscriber` |
| Понять, какие профили оборудования назначены объектам | поля `ground_terminal_profile_key`, `relay_payload_profile_key`, `user_beam_profile_key`, `feeder_beam_profile_key` плюс таблицы профилей |
| Повторить состав старого запуска | `inventory_v2.simulation_request_snapshot` и `simulation_request_snapshot_item` |
| Посмотреть список расчетов | `topo.simulation` |
| Получить движение объектов по времени | `topo.node`, `topo.simulation_time`, `topo.node_position` |
| Собрать технический граф связей | `topo.edge` |
| Посмотреть лучи и их состояние по кадрам | `topo.ray` и `topo.connected_ray` |
| Посмотреть обслуживание теоретических абонентов | `topo.theoretical_subscriber` и `topo.theoretical_subscriber_service` |
| Разобрать радиобюджет | `topo.link_budget_input_snapshot` и `topo.link_budget_result` |
| Быстро взять готовый кадр топологии целиком | `topo.topology_snapshot` |

## 3. Как я разделяю данные

Для себя я делю базу на три слоя.

Первый слой - исходные данные. Это объекты, каталоги, профили и ограничения до запуска модели. В основном они лежат в `inventory_v2`.

Второй слой - снимок запуска. Это то, с каким набором объектов и настроек модель была запущена. Он нужен, чтобы потом не гадать, что именно считалось.

Третий слой - результаты расчета. Это уже `topo`: что получилось после построения сети, геометрии, активных связей, лучей, сервисов и радиобюджета.

## 4. Основные таблицы `inventory_v2`

### `selection_catalog` и `*_catalog_member`

Каталоги нужны, чтобы быстро брать готовые наборы объектов: спутники, HAPS, шлюзы, VSAT, теоретические абоненты и так далее.

Сначала смотрите:

```sql
select catalog_id, kind, catalog_key, name
from inventory_v2.selection_catalog
order by kind, catalog_key;
```

Потом переходите в таблицы состава каталога:

- `gateway_catalog_member`
- `target_catalog_member`
- `satellite_catalog_member`
- `haps_catalog_member`
- `vsat_catalog_member`
- `theoretical_subscriber_catalog_member`

### Таблицы объектов

Основные таблицы объектов:

- `inventory_v2.gateway`
- `inventory_v2.target`
- `inventory_v2.satellite`
- `inventory_v2.haps`
- `inventory_v2.vsat`
- `inventory_v2.theoretical_subscriber`

Здесь лучше смотреть исходные координаты, имена, ключи, профили и ограничения. Если нужно понять, что за объект был до расчета, искать надо именно здесь, а не в `topo.node`.

### Профили

Профили лежат в:

- `inventory_v2.ground_terminal_profile`
- `inventory_v2.relay_payload_profile`
- `inventory_v2.profile_assignment_rule`

Это важно для радиочасти. В объектах обычно хранится ключ профиля, а сами параметры профиля лежат отдельно.

### Снимки запусков

Для повторения состава запуска смотрите:

- `inventory_v2.simulation_request_snapshot`
- `inventory_v2.simulation_request_snapshot_item`

Это надежнее, чем брать текущий каталог, потому что каталог со временем может измениться, а snapshot фиксирует состояние на момент запуска.

## 5. Основные таблицы `topo`

### `topo.simulation`

Это список запусков. Отсюда обычно начинается любой разбор результата.

```sql
select simulation_uuid, created_at, name, status
from topo.simulation
order by created_at desc
limit 20;
```

`simulation_uuid` потом используется почти во всех запросах.

### `topo.node`

Это общий список узлов расчетного графа. Здесь будут спутники, HAPS, шлюзы, лучи, VSAT, теоретические абоненты и служебные узлы.

```sql
select node_type, count(*) as count
from topo.node
where simulation_uuid = :simulation_uuid
group by node_type
order by count desc;
```

### `topo.simulation_time` и `topo.node_position`

Эти таблицы нужны для движения объектов по времени.

```sql
select t.time_index, t.time_msec, p.node_id, p.x, p.y, p.z
from topo.node_position p
join topo.simulation_time t
  on t.simulation_uuid = p.simulation_uuid
 and t.time_index = p.time_index
where p.simulation_uuid = :simulation_uuid
order by t.time_index, p.node_id;
```

Координаты `x`, `y`, `z` хранятся в ECEF, в километрах.

### `topo.edge`

Это технический граф связей. Если нужно собрать путь вида `Satellite -> Ray -> VSAT`, начинать надо отсюда.

```sql
select time_index, begin_node_id, end_node_id, edge_type
from topo.edge
where simulation_uuid = :simulation_uuid
order by time_index, edge_id;
```

### `topo.ray` и `topo.connected_ray`

`topo.ray` - паспорт луча.

`topo.connected_ray` - состояние луча по времени: кто источник, сколько соединений, есть ли целевой узел, какая длина трассы, какие served node ids и так далее.

```sql
select cr.time_index,
       src.name as origin_name,
       dst.name as target_name,
       cr.ray_kind,
       cr.connect_count,
       cr.served_node_count,
       cr.activity_kind,
       cr.length,
       cr.elevation
from topo.connected_ray cr
join topo.node src
  on src.simulation_uuid = cr.simulation_uuid
 and src.node_id = cr.origin_node_id
left join topo.node dst
  on dst.simulation_uuid = cr.simulation_uuid
 and dst.node_id = cr.target_node_id
where cr.simulation_uuid = :simulation_uuid
order by cr.time_index, cr.ray_node_id;
```

Важный момент: поле `alt` в `connected_ray` не стоит читать как обычную высоту цели. Для прикладного анализа лучше использовать `length`, `beam_target_offset_rad`, `pointing_x/y/z` и координаты узлов из `node_position`.

### `topo.theoretical_subscriber` и `topo.theoretical_subscriber_service`

Эти таблицы нужны для слоя теоретических абонентов.

`theoretical_subscriber` хранит самих абонентов: ключ, регион, координаты, профиль, высоту площадки и metadata.

`theoretical_subscriber_service` показывает, кто и как обслуживает абонента по кадрам: провайдер, луч, частота, емкость, spectral efficiency, признак primary service.

### `topo.link_budget_input_snapshot` и `topo.link_budget_result`

Это радиобюджет.

`link_budget_input_snapshot` хранит входной контракт для расчета бюджета линии. Это удобно, когда надо проверить, какие параметры реально ушли в расчет.

`link_budget_result` хранит результат: доступность, причина, slant range, elevation, losses, C/N, C/N0, Eb/N0, modcod, margin и связанные metadata.

### `topo.topology_snapshot`

Это готовый JSON-снимок кадра. Он полезен, когда не хочется заново собирать картину из `node`, `edge`, `ray` и `connected_ray`.

```sql
select time_index, snapshot
from topo.topology_snapshot
where simulation_uuid = :simulation_uuid
order by time_index;
```

## 6. Несколько быстрых запросов

Последние запуски:

```sql
select simulation_uuid, created_at, name, status
from topo.simulation
order by created_at desc
limit 20;
```

Размер одного запуска:

```sql
select s.simulation_uuid,
       s.name,
       s.created_at,
       (select count(*) from topo.node n where n.simulation_uuid = s.simulation_uuid) as node_count,
       (select count(*) from topo.edge e where e.simulation_uuid = s.simulation_uuid) as edge_count,
       (select count(*) from topo.connected_ray r where r.simulation_uuid = s.simulation_uuid) as connected_ray_count,
       (select count(*) from topo.link_budget_result b where b.simulation_uuid = s.simulation_uuid) as link_budget_result_count
from topo.simulation s
order by s.created_at desc;
```

Проверка таблиц:

```sql
select table_schema, table_name
from information_schema.tables
where table_schema in ('inventory_v2', 'topo', 'topo_v2', 'inventory_explicit')
order by table_schema, table_name;
```

Подсчет theoretical subscribers:

```sql
select count(*) from topo.theoretical_subscriber;
```

Подсчет результатов радиобюджета:

```sql
select count(*) from topo.link_budget_result;
```

## 7. Если что-то не открывается

Первое, что я бы проверил, - размер файла дампа. Он должен быть большим. Если файл маленький, значит Git LFS не подтянул настоящий объект.

Команды:

```powershell
git lfs install
git lfs pull
```

После этого снова проверьте размер `.dump` и повторите восстановление.
