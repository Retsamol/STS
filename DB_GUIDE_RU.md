# Руководство по схеме `topo`

Здесь описана схема `topo`, то есть результаты уже выполненных прогонов. В этой базе нужно смотреть построенную топологию, состояние узлов и лучей по времени, результаты traffic evaluation, распределение ресурсов, очереди и результаты радиобюджета.

Обычно я начинаю работу с таблицы `topo.simulation`. Она дает список прогонов и главный ключ для дальнейшего анализа - `simulation_uuid`.

```sql
select simulation_uuid, created_at, name, status
from topo.simulation
order by created_at desc
limit 20;
```

После этого выбранный `simulation_uuid` используется в остальных запросах.

## Основная логика чтения

Схема устроена вокруг одного прогона. `topo.simulation` задает контейнер результата, `topo.simulation_time` хранит временную сетку, `topo.node` хранит все узлы расчетного графа, а `topo.node_position` дает координаты этих узлов по шагам времени.

Связи между узлами находятся в `topo.edge`. Это технический граф, поэтому в нем нормально видеть промежуточные узлы типа `Ray`: например путь может идти как `Satellite -> Ray -> VSAT`. Если нужен готовый кадр без ручной сборки из нескольких таблиц, удобнее читать `topo.topology_snapshot`.

Лучи описаны двумя таблицами. `topo.ray` хранит паспорт луча, а `topo.connected_ray` показывает его состояние по времени: источник, цель, число подключений, обслуженные узлы, направление, длину трассы и другие расчетные признаки.

Для теоретических абонентов используются `topo.theoretical_subscriber` и `topo.theoretical_subscriber_service`. Для grouped subscribers есть `topo.grouped_subscriber`, `topo.grouped_subscriber_service`, `topo.theoretical_subscriber_group` и `topo.theoretical_subscriber_cohort`.

Результаты потоков лежат в `topo.traffic_flow_result`. Если нужно понять, какие участки пути и ресурсы были задействованы, используется `topo.traffic_resource_allocation`. Очереди, задержки и потери по узлам лежат в `topo.node_queue_state`.

Результаты радиобюджета находятся в `topo.link_budget_result`.

## Быстрые запросы

Размеры прогонов удобно оценивать так:

```sql
select s.simulation_uuid,
       s.name,
       s.created_at,
       (select count(*) from topo.node n where n.simulation_uuid = s.simulation_uuid) as node_count,
       (select count(*) from topo.edge e where e.simulation_uuid = s.simulation_uuid) as edge_count,
       (select count(*) from topo.connected_ray r where r.simulation_uuid = s.simulation_uuid) as connected_ray_count,
       (select count(*) from topo.traffic_flow_result f where f.simulation_uuid = s.simulation_uuid) as traffic_flow_result_count,
       (select count(*) from topo.link_budget_result b where b.simulation_uuid = s.simulation_uuid) as link_budget_result_count
from topo.simulation s
order by s.created_at desc;
```

Состав узлов выбранного прогона:

```sql
select node_type, count(*) as count
from topo.node
where simulation_uuid = :simulation_uuid
group by node_type
order by count desc;
```

Координаты узлов по времени:

```sql
select t.time_index,
       t.time_msec,
       p.node_id,
       p.x,
       p.y,
       p.z
from topo.node_position p
join topo.simulation_time t
  on t.simulation_uuid = p.simulation_uuid
 and t.time_index = p.time_index
where p.simulation_uuid = :simulation_uuid
order by t.time_index, p.node_id;
```

Координаты `x`, `y`, `z` хранятся в ECEF, в километрах.

Технический граф связей:

```sql
select time_index, begin_node_id, end_node_id, edge_type
from topo.edge
where simulation_uuid = :simulation_uuid
order by time_index, edge_id;
```

Состояние лучей:

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

Поле `connected_ray.alt` не стоит читать как обычную высоту цели. Для прикладного анализа лучше опираться на `length`, `beam_target_offset_rad`, `pointing_x`, `pointing_y`, `pointing_z` и координаты узлов из `node_position`.

Готовые JSON-снимки топологии:

```sql
select time_index, snapshot
from topo.topology_snapshot
where simulation_uuid = :simulation_uuid
order by time_index;
```

## Traffic results

Результат по потокам:

```sql
select flow_id,
       time_index,
       src_node_id,
       dst_node_id,
       requested_rate_mbps,
       served_rate_mbps,
       unserved_rate_mbps,
       lost_mbps,
       queue_delay_ms,
       reason
from topo.traffic_flow_result
where simulation_uuid = :simulation_uuid
order by time_index, flow_id;
```

Причины необслуженного трафика:

```sql
select reason, count(*) as count
from topo.traffic_flow_result
where simulation_uuid = :simulation_uuid
group by reason
order by count desc;
```

Распределение потока по ресурсам:

```sql
select flow_id,
       time_index,
       segment_index,
       begin_node_id,
       end_node_id,
       segment_kind,
       served_rate_mbps,
       link_type,
       media,
       capacity_mbps
from topo.traffic_resource_allocation
where simulation_uuid = :simulation_uuid
order by time_index, flow_id, segment_index;
```

Очереди и потери:

```sql
select flow_id,
       time_index,
       node_id,
       processing_capacity_mbps,
       buffer_occupancy_mbit,
       queued_mbit,
       lost_mbps,
       queue_delay_ms
from topo.node_queue_state
where simulation_uuid = :simulation_uuid
order by time_index, flow_id, node_id;
```

## Grouped subscribers

Развернутые grouped subscribers:

```sql
select node_id,
       subscriber_key,
       group_key,
       subscriber_count,
       distribution_rule,
       radio_profile,
       ground_terminal_profile_key
from topo.grouped_subscriber
where simulation_uuid = :simulation_uuid
order by group_key, node_id;
```

Их обслуживание по кадрам:

```sql
select subscriber_node_id,
       time_index,
       connected,
       is_primary,
       provider_node_id,
       provider_type,
       ray_node_id,
       capacity_mbps
from topo.grouped_subscriber_service
where simulation_uuid = :simulation_uuid
order by time_index, subscriber_node_id, service_rank;
```

`topo.theoretical_subscriber_group` и `topo.theoretical_subscriber_cohort` помогают понять, как группа представлена в расчете, а `topo.grouped_subscriber` и `topo.grouped_subscriber_service` показывают уже расчетный результат обслуживания.

## Радиобюджет

Результаты радиобюджета:

```sql
select endpoint_node_id,
       time_index,
       segment_kind,
       endpoint_kind,
       relay_node_id,
       available,
       reason,
       slant_range_km,
       elevation_deg,
       cn_db,
       cn0_dbhz,
       ebn0_db,
       modcod,
       margin_db
from topo.link_budget_result
where simulation_uuid = :simulation_uuid
order by time_index, endpoint_node_id, segment_kind;
```

## Если дамп не восстанавливается

В первую очередь проверьте размер файла:

```powershell
Get-Item .\db\exports\topo_results_2026-05-11.dump
```

Файл должен быть большим, около 1.57 ГБ. Если он маленький, значит Git LFS не подтянул настоящий объект:

```powershell
git lfs install
git lfs pull
```
