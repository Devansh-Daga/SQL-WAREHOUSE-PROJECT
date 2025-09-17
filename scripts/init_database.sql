/* 
=============================================
CREATE DATAASE AND SCHEMAS
=============================================

Script Purpose:
  this script creates a new database named 'DataWarehouse' after checking if it already exists.
  if it already exists then it is dropped and recreated. Additionally, the script setup three schemas
  within the database : 'Bronze','Silver','Gold'

Warning : 
  Running this script will drop the entire 'DataWarehouse' database if it exists.
All data in the database will be permanently deleted> Proceed with caution.
and ensure you have proper backups before running the script.

*/


--- Dropping the database 'DataWarehouse'
DROP DATABASE IF EXISTS DataWarehouse;

--- CREATE  DATABASE 'DataWarehouse'
CREATE DATABASE DataWarehouse;

--- Create Schemas
CREATE SCHEMA Bronze;

CREATE SCHEMA Silver;

CREATE SCHEMA Gold;

