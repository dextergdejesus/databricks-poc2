USE CATALOG ceo_brz_dev;
USE SCHEMA dbo_managed;
-- DROP TABLE IF EXISTS `ceo_brz_dev`.`dbo_managed`.`entity`;
CREATE TABLE `ceo_brz_dev`.`dbo_managed`.`entity`(
  EntityId INT,
  EntityName STRING,
  BronzeFileName STRING,
  BronzePath STRING,
  BronzeFormat STRING,
  BronzeCatalog STRING,
  BronzeSchema STRING,
  BronzeTableName STRING,
  SilverFormat STRING,
  SilverCatalog STRING,
  SilverSchema STRING,
  SilverTableName STRING,
  LoadType STRING,
  LastExtractionDate TIMESTAMP,
  IsFileAvailableInBronze BOOLEAN
);

-- DROP TABLE IF EXISTS `ceo_brz_dev`.`dbo_managed`.`entity_schema`;
CREATE TABLE `ceo_brz_dev`.`dbo_managed`.`entity_schema` (
  EntitySchemaId INT,
  EntityId STRING,
  BronzeColumnName STRING,
  BronzeDataType STRING,
  SilverColumnName STRING,
  SilverDataType STRING,
  IsPrimaryKey BOOLEAN,
  CleansingRule STRING)
USING DELTA;

-- DROP TABLE IF EXISTS `ceo_brz_dev`.`dbo_managed`.`entity_log`;
CREATE TABLE `ceo_brz_dev`.`dbo_managed`.`entity_log` (
  LogDate DATE,
  ExecutionDate TIMESTAMP,
  Stage STRING,
  EntityName STRING,
  Operation STRING,
  Status STRING,
  BronzeCount BIGINT,
  SilverCount BIGINT,
  GoldCount BIGINT,
  ErrorMessage STRING)
USING DELTA
PARTITIONED BY (LogDate);
