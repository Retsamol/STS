ALTER TABLE topo.node
  DROP CONSTRAINT IF EXISTS node_node_type_check;

ALTER TABLE topo.node
  ADD CONSTRAINT node_node_type_check
  CHECK (
    node_type = ANY (
      ARRAY[
        'CGS'::text,
        'Target'::text,
        'Satellite'::text,
        'HAPS'::text,
        'Ray'::text,
        'VSAT'::text,
        'ImportantPlace'::text,
        'Region'::text,
        'TheoreticalSubscriber'::text
      ]
    )
  );

ALTER TABLE topo.connected_ray
  ADD COLUMN IF NOT EXISTS origin_node_type text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS ray_kind text NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS aperture double precision NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS connect_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS target_node_id integer,
  ADD COLUMN IF NOT EXISTS is_targeted boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS served_node_count integer NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS activity_kind text NOT NULL DEFAULT 'endpoint_connectivity',
  ADD COLUMN IF NOT EXISTS served_node_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS served_node_types jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS beam_target_offset_rad double precision,
  ADD COLUMN IF NOT EXISTS pointing_x double precision,
  ADD COLUMN IF NOT EXISTS pointing_y double precision,
  ADD COLUMN IF NOT EXISTS pointing_z double precision;

ALTER TABLE topo.simulation_time
  ADD COLUMN IF NOT EXISTS time_utc timestamptz;

ALTER TABLE topo.ray
  ADD COLUMN IF NOT EXISTS center_frequency_ghz double precision,
  ADD COLUMN IF NOT EXISTS bandwidth_mhz double precision;

CREATE TABLE IF NOT EXISTS topo.theoretical_subscriber (
  simulation_uuid uuid NOT NULL,
  node_id integer NOT NULL,
  subscriber_key text NOT NULL,
  subject_code integer NOT NULL,
  subject_name text NOT NULL,
  federal_district text NOT NULL,
  grid_cell_id text NOT NULL,
  seed_version text NOT NULL,
  la double precision NOT NULL,
  lo double precision NOT NULL,
  alt double precision NOT NULL,
  site_alt_m double precision,
  antenna_height_agl_m double precision NOT NULL,
  radio_profile text NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS topo.theoretical_subscriber_service (
  simulation_uuid uuid NOT NULL,
  subscriber_node_id integer NOT NULL,
  time_index integer NOT NULL,
  service_rank integer NOT NULL,
  connected boolean NOT NULL,
  is_primary boolean NOT NULL,
  provider_node_id integer,
  provider_type text,
  ray_node_id integer,
  ray_origin_node_id integer,
  ray_origin_type text,
  target_node_id integer,
  frequency_id integer,
  aperture_rad double precision,
  path_length_km double precision,
  beam_target_offset_rad double precision,
  radio_profile text NOT NULL,
  site_alt_m double precision,
  antenna_height_agl_m double precision,
  spectral_efficiency_bps_hz double precision,
  capacity_mbps double precision,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_theoretical_subscriber_service_time
  ON topo.theoretical_subscriber_service(simulation_uuid, time_index);

CREATE INDEX IF NOT EXISTS ix_theoretical_subscriber_service_provider
  ON topo.theoretical_subscriber_service(simulation_uuid, provider_node_id, time_index);

CREATE TABLE IF NOT EXISTS topo.link_budget_input_snapshot (
  simulation_uuid uuid NOT NULL,
  endpoint_node_id integer NOT NULL,
  time_index integer NOT NULL,
  segment_kind text NOT NULL,
  endpoint_kind text NOT NULL,
  relay_node_id integer NOT NULL,
  relay_type text NOT NULL,
  gateway_node_id integer,
  ray_node_id integer,
  target_node_id integer,
  input_payload jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_link_budget_input_snapshot_time
  ON topo.link_budget_input_snapshot(simulation_uuid, time_index, segment_kind);

CREATE TABLE IF NOT EXISTS topo.link_budget_result (
  simulation_uuid uuid NOT NULL,
  endpoint_node_id integer NOT NULL,
  time_index integer NOT NULL,
  segment_kind text NOT NULL,
  endpoint_kind text NOT NULL,
  relay_node_id integer NOT NULL,
  relay_type text NOT NULL,
  gateway_node_id integer,
  ray_node_id integer,
  target_node_id integer,
  available boolean NOT NULL,
  reason text NOT NULL,
  slant_range_km double precision,
  elevation_deg double precision,
  beam_target_offset_rad double precision,
  radial_velocity_mps double precision,
  doppler_hz double precision,
  lfsl_db double precision,
  lrain_db double precision,
  latm_gas_db double precision,
  latm_cloud_db double precision,
  lscint_db double precision,
  r001_mm_h double precision,
  noise_temperature_k double precision,
  cn_db double precision,
  cn0_dbhz double precision,
  ebn0_db double precision,
  spectral_efficiency_bps_hz double precision,
  info_rate_kbps double precision,
  symbol_rate_kbaud double precision,
  modcod text,
  margin_db double precision,
  profile_keys jsonb NOT NULL,
  metadata jsonb NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_link_budget_result_time
  ON topo.link_budget_result(simulation_uuid, time_index, segment_kind);

CREATE INDEX IF NOT EXISTS ix_link_budget_result_endpoint
  ON topo.link_budget_result(simulation_uuid, endpoint_node_id, time_index);
