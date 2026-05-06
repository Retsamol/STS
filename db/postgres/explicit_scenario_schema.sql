CREATE SCHEMA IF NOT EXISTS inventory_explicit;

CREATE TABLE IF NOT EXISTS inventory_explicit.ground_terminal_profile (
  profile_key text PRIMARY KEY,
  station_kind text NOT NULL,
  tx_power_dbw double precision,
  tx_antenna_diameter_m double precision,
  tx_antenna_gain_dbi double precision,
  tx_center_frequency_ghz double precision,
  tx_bandwidth_mhz double precision,
  tx_polarization_deg double precision,
  tx_waveguide_loss_db double precision,
  rx_antenna_diameter_m double precision,
  rx_antenna_gain_dbi double precision,
  rx_center_frequency_ghz double precision,
  rx_bandwidth_mhz double precision,
  rx_polarization_deg double precision,
  rx_waveguide_loss_db double precision,
  lna_noise_temperature_k double precision,
  rolloff double precision,
  lm_db double precision,
  if_to_rf_degradation_db double precision,
  rain_probability_percent double precision,
  off_axis_loss_db_per_rad double precision,
  antenna_pattern_reference text,
  source_name text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_explicit.relay_payload_profile (
  profile_key text PRIMARY KEY,
  relay_mode text NOT NULL DEFAULT 'transparent_relay',
  eirp_sat_dbw double precision,
  gt_dbk double precision,
  sfd_dbw_m2 double precision,
  ibo_db double precision,
  obo_db double precision,
  npr_db double precision,
  tx_power_dbw double precision,
  tx_center_frequency_ghz double precision,
  tx_bandwidth_mhz double precision,
  tx_antenna_diameter_m double precision,
  tx_antenna_gain_dbi double precision,
  tx_polarization_deg double precision,
  tx_waveguide_loss_db double precision,
  rx_center_frequency_ghz double precision,
  rx_bandwidth_mhz double precision,
  rx_antenna_diameter_m double precision,
  rx_antenna_gain_dbi double precision,
  rx_polarization_deg double precision,
  rx_waveguide_loss_db double precision,
  rx_noise_temperature_k double precision,
  off_axis_loss_db_per_rad double precision,
  antenna_pattern_reference text,
  source_name text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_explicit.gateway (
  gateway_key text PRIMARY KEY,
  name text NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  site_alt_m double precision,
  antenna_height_agl_m double precision,
  radio_profile text,
  ground_terminal_profile_key text,
  role text,
  connect_limit integer,
  capacity_mbps double precision,
  bandwidth_mhz double precision,
  spectral_efficiency_bps_hz double precision,
  source_name text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_explicit.target (
  target_key text PRIMARY KEY,
  name text NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  frequency integer NOT NULL,
  priority double precision,
  site_alt_m double precision,
  antenna_height_agl_m double precision,
  ground_terminal_profile_key text,
  source_name text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_explicit.satellite (
  satellite_key text PRIMARY KEY,
  name text NOT NULL,
  tle_line1 text NOT NULL,
  tle_line2 text NOT NULL,
  radio_profile text,
  user_beam_profile_key text,
  feeder_beam_profile_key text,
  connection_min integer,
  beam_layout_mode text,
  dynamic_ray_count integer,
  dynamic_ray_aperture_deg double precision,
  sat_haps_ray_count integer,
  source_name text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_explicit.haps (
  haps_key text PRIMARY KEY,
  name text NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  alt_m double precision NOT NULL,
  radio_profile text,
  user_beam_profile_key text,
  feeder_beam_profile_key text,
  connection_min integer,
  source_name text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_explicit.vsat (
  vsat_key text PRIMARY KEY,
  name text NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  region_code integer NOT NULL,
  site_alt_m double precision,
  antenna_height_agl_m double precision,
  radio_profile text,
  ground_terminal_profile_key text,
  role text,
  connect_limit integer,
  source_name text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_explicit.theoretical_subscriber (
  subscriber_key text PRIMARY KEY,
  name text NOT NULL,
  lat double precision NOT NULL,
  lon double precision NOT NULL,
  subject_code integer NOT NULL,
  subject_name text NOT NULL,
  federal_district text NOT NULL,
  grid_cell_id text NOT NULL,
  seed_version text NOT NULL,
  site_alt_m double precision,
  ground_terminal_profile_key text,
  is_active boolean NOT NULL DEFAULT true,
  source_name text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_explicit.satellite_resource_limit (
  satellite_key text PRIMARY KEY,
  max_user_links integer,
  max_feeder_links integer,
  max_interobject_links integer
);

CREATE TABLE IF NOT EXISTS inventory_explicit.haps_resource_limit (
  haps_key text PRIMARY KEY,
  max_user_links integer,
  max_feeder_links integer,
  max_haps_links integer,
  angle_rad double precision,
  beam_angle_rad double precision,
  beam_angle_deg double precision,
  maxlength double precision
);

CREATE TABLE IF NOT EXISTS inventory_explicit.satellite_allowed_link_type (
  satellite_key text NOT NULL,
  link_type text NOT NULL,
  PRIMARY KEY (satellite_key, link_type)
);

CREATE TABLE IF NOT EXISTS inventory_explicit.haps_allowed_link_type (
  haps_key text NOT NULL,
  link_type text NOT NULL,
  PRIMARY KEY (haps_key, link_type)
);

CREATE TABLE IF NOT EXISTS inventory_explicit.scenario (
  scenario_id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  scenario_key text NOT NULL UNIQUE,
  name text NOT NULL,
  description text,
  status text NOT NULL DEFAULT 'draft',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory_explicit.scenario_timeline (
  scenario_id bigint PRIMARY KEY REFERENCES inventory_explicit.scenario(scenario_id) ON DELETE CASCADE,
  start_time_utc timestamptz NOT NULL,
  end_time_utc timestamptz NOT NULL,
  time_step_sec integer NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory_explicit.scenario_network_settings (
  scenario_id bigint PRIMARY KEY REFERENCES inventory_explicit.scenario(scenario_id) ON DELETE CASCADE,
  selection_mode text NOT NULL,
  earth_model text NOT NULL,
  min_elevation_deg double precision,
  target_elevation_deg double precision,
  connectivity_mode text NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory_explicit.scenario_entity (
  scenario_id bigint NOT NULL REFERENCES inventory_explicit.scenario(scenario_id) ON DELETE CASCADE,
  kind text NOT NULL,
  entity_key text NOT NULL,
  role text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  ordinal integer NOT NULL DEFAULT 0,
  ground_terminal_profile_key text,
  user_beam_profile_key text,
  feeder_beam_profile_key text,
  PRIMARY KEY (scenario_id, kind, entity_key)
);

CREATE TABLE IF NOT EXISTS inventory_explicit.scenario_traffic_flow (
  scenario_id bigint NOT NULL REFERENCES inventory_explicit.scenario(scenario_id) ON DELETE CASCADE,
  flow_key text NOT NULL,
  source_kind text NOT NULL,
  source_key text NOT NULL,
  target_kind text NOT NULL,
  target_key text NOT NULL,
  requested_rate_mbps double precision NOT NULL,
  priority integer NOT NULL DEFAULT 100,
  start_time_utc timestamptz,
  end_time_utc timestamptz,
  PRIMARY KEY (scenario_id, flow_key)
);
