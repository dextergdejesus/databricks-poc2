
-- USE CATALOG ceo_slv_dev;
-- USE SCHEMA taxonomy;
-- DROP TABLE IF EXISTS `ceo_slv_dev`.`taxonomy`.`dim_function_group`;
-- CREATE TABLE `ceo_slv_dev`.`taxonomy`.`dim_function_group` (
--  FunctionGroupId BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
--  FunctionGroup STRING,
--  Description STRING)
-- USING DELTA;

-- DROP TABLE IF EXISTS `ceo_slv_dev`.`taxonomy`.`dim_function_type`;
-- CREATE TABLE `ceo_slv_dev`.`taxonomy`.`dim_function_type` (
--  FunctionTypeId BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
--  FunctionType STRING,
--  Description STRING)
-- USING DELTA;

-- DROP TABLE IF EXISTS `ceo_brz_dev`.`taxonomy`.`dim_program_group_type`;
-- CREATE TABLE `ceo_brz_dev`.`taxonomy`.`dim_program_group_type` (
--  ProgramGroupId BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
--  FunctionGroupId INT,
--  FunctionTypeId INT,
--  ProjectId INT)
-- USING DELTA;

