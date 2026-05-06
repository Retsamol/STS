ALTER TABLE inventory_explicit.satellite_resource_limit
  ADD COLUMN IF NOT EXISTS access_model text;

ALTER TABLE inventory_explicit.haps_resource_limit
  ADD COLUMN IF NOT EXISTS cgs_ray_count integer,
  ADD COLUMN IF NOT EXISTS feeder_ray_count integer;
