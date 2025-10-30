CREATE OR REPLACE VIEW `ceo_analytics_dev`.`dbo`.`vw_budget_spending_by_geography` AS
SELECT
DistrictCensusTractId,
Allocation AS BudgetAllocation,
Expenditure AS BudgetExpenditure,
Variance
FROM    `ceo_slv_dev_managed`.`insights`.`fact_budget_spending_by_geography`