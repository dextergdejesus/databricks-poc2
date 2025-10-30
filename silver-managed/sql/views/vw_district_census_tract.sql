CREATE OR REPLACE VIEW `ceo_analytics_dev`.`dbo`.`vw_district_census_tract` AS
SELECT
        DistrictCensusTractId ,
        GeoId,
        District,
        CensusTract                            
FROM    `ceo_slv_dev_managed`.`insights`.`dim_district_census_tract` 