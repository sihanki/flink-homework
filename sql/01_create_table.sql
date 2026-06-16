-- File: 01_create_table.sql
-- Creates a table for measuring devices (temperature + humidity)

CREATE TABLE IF NOT EXISTS measuring_devices (
    id         VARCHAR(50) PRIMARY KEY,   -- device identifier
    typename   VARCHAR(100) NOT NULL      -- type name of the device
);
