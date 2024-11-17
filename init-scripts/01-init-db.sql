-- Create additional schemas or extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create additional roles or schemas if needed
CREATE SCHEMA IF NOT EXISTS sales_data;

-- Optional: Create initial tables or seed data
-- CREATE TABLE IF NOT EXISTS sales_data.initial_config (
--     id SERIAL PRIMARY KEY,
--     config_key VARCHAR(255) NOT NULL,
--     config_value TEXT
-- );

-- Optional: Insert initial configuration
-- INSERT INTO sales_data.initial_config (config_key, config_value)
-- VALUES 
--     ('etl_version', '1.0.0'),
--     ('last_run', NOW());
