-- Databricks notebook source
SELECT
  region,
  sum('ifc_investment_for_loan_million_usd`) AS total_ifc_loan_investment_million_usd
FROM
  `apj_databricks_hackathon_2025`.`apj_quant_masters_genie_hackathon`.`synthetic_ifc_projects_5000_cleaned`
WHERE
  date_disclosed BETWEEN dateadd(YEAR,-10,current_date()) AND current_date()
GROUP BY
  ALL
ORDER BY
  total_ifc_loan_investment_million_usd DESC

-- COMMAND ----------



-- COMMAND ----------

WITH ranked_projects AS (
  SELECT
    project_name,
    project_number,
    project_url,
    MEASURE(
      total_ifc_investment_approved_by_board_million_usd
    ) AS total_ifc_investment_approved_by_board_million_usd,
    ROW_NUMBER() OVER (
        ORDER BY MEASURE(total_ifc_investment_approved_by_board_million_usd) DESC
      ) AS investment_rank
  FROM
    apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.synthetic_ifc_projects_5000_metric_view
  WHERE
    project_name IS NOT NULL
    AND project_number IS NOT NULL
    AND project_url IS NOT NULL
  GROUP BY
    ALL
)
SELECT
  project_name,
  project_number,
  project_url,
  total_ifc_investment_approved_by_board_million_usd
FROM
  ranked_projects
WHERE
  investment_rank <= 25
ORDER BY
  investment_rank

-- COMMAND ----------

delete table if exists apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.synthetic_ifc_projects_5000_cleaned;

create table apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.synthetic_ifc_projects_5000_cleaned

-- COMMAND ----------

UPDATE apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.synthetic_ifc_projects_5000
SET country = 'Vatican City'
WHERE country = 'Holy See (Vatican City State)'

-- COMMAND ----------

select a.Country, count(1) from 
    apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.synthetic_ifc_projects_5000 a  
left join apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.countries c 
on UPPER(a.country) = UPPER(c.name)
where c.name is null GROUP BY a.Country

-- COMMAND ----------

-- DBTITLE 1,Cell 5
SELECT a.*, c.name as matched_country_name, c.latitude, c.longitude
FROM apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.synthetic_ifc_projects_5000 a
CROSS JOIN apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.countries c
WHERE ai_similarity(a.country, c.name) > 0.8

-- COMMAND ----------


