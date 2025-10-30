CREATE OR REPLACE VIEW `ceo_analytics_dev`.`dbo`.`vw_programs` AS
SELECT
            p.ProjectId                 AS ProgramId,
            p.ProjectName               AS Program,
            ps.FullSummary              AS ProgramDescription
FROM        `ceo_slv_dev_managed`.`arp`.`dim_project` p
LEFT JOIN   `ceo_slv_dev_managed`.`arp`.`dim_project_summary` ps ON
            ps.ProjectId = p.ProjectId