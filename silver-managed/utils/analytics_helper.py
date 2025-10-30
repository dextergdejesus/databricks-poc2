from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.builder.getOrCreate()

def dim_pillar_categories():
    spark = get_spark()
    df = spark.sql("""
        SELECT 
                    xxhash64(concat_ws("|",sg.SpendingGroupId,sc.SpendingCategoryId))               AS SpendingGroupCategoryId,
                    sg.SpendingGroupId                                                              AS SpendingGroupId,
                    CASE
                            WHEN UPPER(LEFT(sg.GroupName,18)) = 'STRATEGIC PILLAR 1'
                            THEN 'FIRST PILLAR'
                            WHEN UPPER(LEFT(sg.GroupName,18)) = 'STRATEGIC PILLAR 2'
                            THEN 'SECOND PILLAR'
                            WHEN UPPER(LEFT(sg.GroupName,18)) = 'STRATEGIC PILLAR 3'
                            THEN 'THIRD PILLAR'
                    END                                                                             AS SpendingGroupOrder,
                    sg.GroupName                                                                    AS SpendingGroup,
                    sc.SpendingCategoryId                                                           AS SpendingCategoryId,
                    LEFT(sc.CategoryName,INSTR(sc.CategoryName," ")-1)                              AS CategoryOrder,
                    RIGHT(sc.CategoryName,LENGTH(sc.CategoryName)-INSTR(sc.CategoryName," "))       AS Category
        FROM        `ceo_slv_dev_managed`.`arp`.`dim_spending_group` sg 
        INNER JOIN  `ceo_slv_dev_managed`.`arp`.`dim_spending_category` sc ON
                    sg.SpendingGroupId = sc.SpendingGroupId
                                           """)
    return df


def fact_budget_spending_by_program ():
    spark = get_spark()
    df = spark.sql("""
        SELECT
                    p.ProjectId                                     AS ProjectId,
                    pc.SpendingGroupCategoryId                      AS SpendingGroupCategoryId,
                    IFNULL(p.RevisedAmount, p.BudgetAmount)         AS BudgetAllocation
        FROM        `ceo_slv_dev_managed`.`arp`.`dim_project` p
        INNER JOIN  `ceo_slv_dev_managed`.`insights`.`dim_pillar_categories` pc ON
                    pc.SpendingCategoryId = p.SpendingCategoryId

                                        """)
    return df

def dim_district_census_tract():
    spark = get_spark()
    df = spark.sql("""
       SELECT xxhash64(concat_ws("|",SupervisorDistrict,GeoId)) AS DistrictCensusTractId,
              GeoId ,
              SupervisorDistrict AS District,
              RIGHT(GeoId,6) AS CensusTract
        FROM ceo_slv_dev_managed.arp.dim_census_tract 
                                   
                                   """)
    return df

def fact_budget_spending_by_geography():
    spark = get_spark()
    df = spark.sql("""
     SELECT dct.DistrictCensusTractId ,
            qmt.Allocation,
            qmt.Expenditure,
            (qmt.Allocation- qmt.Expenditure) AS Variance
     FROM ceo_slv_dev_managed.arp.fact_quarterly_census_amount AS qmt
     INNER JOIN ceo_slv_dev_managed.arp.dim_census_tract AS ct
     ON 
            ct.CensusTractId = qmt.CensusTractId
    INNER JOIN ceo_slv_dev_managed.insights.dim_district_census_tract AS dct
     ON 
            dct.GeoId = ct.GeoId                              
                                   """)
    return df  

def dim_district_census_tract():
    spark = get_spark()
    df = spark.sql("""
     SELECT
                    p.ProjectId                                     AS ProjectId,
                    pc.SpendingGroupCategoryId                      AS SpendingGroupCategoryId,
                    IFNULL(p.RevisedAmount, p.BudgetAmount)         AS BudgetAllocation
        FROM        `ceo_slv_dev_managed`.`arp`.`dim_project` p
        INNER JOIN  `ceo_slv_dev_managed`.`insights`.`dim_pillar_categories` pc ON
                    pc.SpendingCategoryId = p.SpendingCategoryId
                                        """)                              
    return df

def dim_treasury_expenditure():
    spark = get_spark()
    df = spark.sql("""
     SELECT
    xxhash64(concat_ws('|',
            ecg.ExpenditureCategoryGroupId,
            ec.ExpenditureCategoryId
     )) AS TreasuryExpenditureId,
            ecg.ExpenditureCategoryGroupId,
            ecg.ExpenditureCategoryGroup AS ExpenditureCategoryGroup,
            ec.ExpenditureCategoryId,
             ec.ExpenditureCategory AS ExpenditureCategory
        FROM ceo_slv_dev.arp.dim_expenditure_category_group AS ecg
        INNER JOIN ceo_slv_dev.arp.dim_expenditure_category AS ec
         ON ecg.ExpenditureCategoryGroupId = ec.ExpenditureCategoryGroupId
        INNER JOIN ceo_slv_dev.arp.dim_project AS p
        ON p.ExpenditureCategoryId = ec.ExpenditureCategoryId;

                                        """)                              
    return df




    
