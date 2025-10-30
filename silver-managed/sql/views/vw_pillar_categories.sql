CREATE OR REPLACE VIEW `ceo_analytics_dev`.`dbo`.`vw_pillar_categories` AS
SELECT
        SpendingGroupCategoryId         AS PillarCategoryId,
        SpendingGroupOrder              AS PillarOrder,
        SpendingGroup                   AS Pillar,
        CategoryOrder                   AS CategoryOrder,
        Category                        AS Category
FROM    `ceo_slv_dev_managed`.`insights`.`dim_pillar_categories` 
