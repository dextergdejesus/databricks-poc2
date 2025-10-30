# Databricks notebook source
# MAGIC %sql
# MAGIC -- use catalog ceo_brz_dev;
# MAGIC -- select * from dbo_managed.entity
# MAGIC SELECT fg.FunctionGroupId,ft.FunctionTypeId,p.ProjectId FROM (SELECT FunctionGroupId FROM `ceo_slv_dev_managed`.`taxonomy`.`dim_function_group` WHERE FunctionGroup ='Administration') AS fg,(SELECT FunctionTypeId FROM `ceo_slv_dev_managed`.`taxonomy`.`dim_function_type` WHERE FunctionType ='Administrative Management') as ft,(SELECT ProjectId FROM `ceo_slv_dev_managed`.`arp`.`dim_project` WHERE ProjectName ='Administrative Services') as p WHERE fg.FunctionGroupId IS NOT NULL AND ft.FunctionTypeId IS NOT NULL AND p.ProjectId IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT 
# MAGIC --                 xxhash64(concat_ws("|",sg.SpendingGroupId,sc.SpendingCategoryId))               AS SpendingGroupCategoryId,
# MAGIC --                 sg.SpendingGroupId                                                              AS SpendingGroupId,
# MAGIC --                 CASE
# MAGIC --                         WHEN UPPER(LEFT(sg.GroupName,18)) = 'STRATEGIC PILLAR 1'
# MAGIC --                         THEN 'FIRST PILLAR'
# MAGIC --                         WHEN UPPER(LEFT(sg.GroupName,18)) = 'STRATEGIC PILLAR 2'
# MAGIC --                         THEN 'SECOND PILLAR'
# MAGIC --                         WHEN UPPER(LEFT(sg.GroupName,18)) = 'STRATEGIC PILLAR 3'
# MAGIC --                         THEN 'THIRD PILLAR'
# MAGIC --                 END                                                                             AS SpendingGroupOrder,
# MAGIC --                 sg.GroupName                                                                    AS SpendingGroup,
# MAGIC --                 sc.SpendingCategoryId                                                           AS SpendingCategoryId,
# MAGIC --                 LEFT(sc.CategoryName,INSTR(sc.CategoryName," ")-1)                              AS CategoryOrder,
# MAGIC --                 RIGHT(sc.CategoryName,LENGTH(sc.CategoryName)-INSTR(sc.CategoryName," "))       AS Category
# MAGIC -- FROM            `ceo_slv_dev_managed`.`arp`.`dim_spending_group` sg 
# MAGIC -- INNER JOIN      `ceo_slv_dev_managed`.`arp`.`dim_spending_category` sc ON
# MAGIC --                 sg.SpendingGroupId = sc.SpendingGroupId
# MAGIC
# MAGIC -- SELECT
# MAGIC --                 p.ProjectId                                     AS ProjectId,
# MAGIC --                 pc.SpendingGroupCategoryId                      AS SpendingGroupCategoryId,
# MAGIC --                 IFNULL(p.RevisedAmount, p.BudgetAmount)         AS BudgetAllocation
# MAGIC -- FROM            `ceo_slv_dev_managed`.`arp`.`dim_project` p
# MAGIC -- INNER JOIN      `ceo_analytics_dev`.`dbo`.`dim_pillar_categories` pc ON
# MAGIC --                 pc.SpendingCategoryId = p.SpendingCategoryId
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select left("1.1 sparksql",instr("1.1 sparksql"," ")-1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT xxhash64(concat_ws("|",SupervisorDistrict,GeoId)) AS DistrictCensusTractId,
# MAGIC GeoId ,
# MAGIC SupervisorDistrict AS District,
# MAGIC RIGHT(GeoId,6) AS CesusTract
# MAGIC FROM ceo_slv_dev_managed.arp.dim_census_tract 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dct.DistrictCensusTractId ,
# MAGIC qmt.Allocation,
# MAGIC qmt.Expenditure,
# MAGIC (qmt.Allocation- qmt.Expenditure) AS Variance
# MAGIC FROM ceo_slv_dev_managed.arp.fact_quarterly_census_amount AS qmt
# MAGIC INNER JOIN ceo_slv_dev_managed.arp.dim_census_tract AS ct
# MAGIC ON 
# MAGIC ct.CensusTractId = qmt.CensusTractId
# MAGIC INNER JOIN ceo_slv_dev_managed.insights.dim_district_census_tract AS dct
# MAGIC ON 
# MAGIC dct.GeoId = ct.GeoId
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,US 8-10 Select query for Silver output
# MAGIC %sql
# MAGIC SELECT
# MAGIC     p.ProjectId,
# MAGIC     d.DepartmentId,
# MAGIC     sc.SpendingGroupCategoryId,
# MAGIC     te.TreasuryExpenditureId,
# MAGIC     IFNULL(p.RevisedAmount, p.BudgetAmount) AS BudgetAllocation
# MAGIC FROM  ceo_slv_dev_managed.arp.dim_project  AS p
# MAGIC INNER JOIN  ceo_slv_dev_managed.arp.dim_department  AS d   
# MAGIC ON p.DepartmentId = d.DepartmentId
# MAGIC INNER JOIN  ceo_slv_dev_managed.insights.dim_pillar_categories  AS sc 
# MAGIC ON p.SpendingCategoryId = sc.SpendingCategoryId
# MAGIC INNER JOIN  ceo_slv_dev_managed.insights.dim_treasury_expenditure  AS te  
# MAGIC ON p.TreasuryExpenditureId = te.TreasuryExpenditureId
# MAGIC
