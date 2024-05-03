/* Create database */
DROP DATABASE IF EXISTS ordinals;

DO $$
BEGIN
  IF NOT EXISTS(SELECT FROM pg_catalog.pg_roles WHERE rolname = 'backend') THEN
    -- Create role with password and grant the appropriate permissions to that role
CREATE ROLE backend WITH ENCRYPTED PASSWORD 'looks-backend' LOGIN;
ALTER ROLE backend CREATEDB;
END IF;
END
$$;

CREATE DATABASE ordinals WITH OWNER backend;

GRANT ALL PRIVILEGES ON DATABASE ordinals to backend;