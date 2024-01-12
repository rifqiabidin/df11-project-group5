CREATE MATERIALIZED VIEW `df11-group5.dwh_final_project.housing_bank_marketing` AS (
    SELECT
        f.id,
        f.join_yearmonth,
        da.age_group,
        dj.job_type,
        de.education_type,
        dm.marital_type,
        f.has_housing,
    FROM
        `df11-group5.dwh_final_project.partclust_fact_bank_marketing` f
    JOIN `df11-group5.dwh_final_project.dim_age_group` da on f.age_group_id = da.age_group_id
    JOIN `df11-group5.dwh_final_project.dim_job` dj on dj.job_id = f.job_id
    JOIN `df11-group5.dwh_final_project.dim_education` de on de.education_id = f.education_id
    JOIN `df11-group5.dwh_final_project.dim_marital` dm on dm.marital_id = f.marital_id
);