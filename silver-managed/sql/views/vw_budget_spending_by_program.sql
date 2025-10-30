CREATE VIEW `ceo_analytics_dev`.`dbo`.`vw_budget_spending_by_program` AS
SELECT 
        ProjectId                 AS ProgramId,
        SpendingGroupCategoryId   AS PillarCategoryId,
        BudgetAllocation          AS BudgetAllocation
FROM    `ceo_slv_dev_managed`.`insights`.`fact_budget_spending_by_program`