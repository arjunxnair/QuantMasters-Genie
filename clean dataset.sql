-- Databricks notebook source
DROP TABLE IF EXISTS apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.synthetic_ifc_projects_5000_cleaned;

CREATE table apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.synthetic_ifc_projects_5000_cleaned as 

SELECT
  `Date Disclosed` AS date_disclosed,
  `Project Name` AS project_name,
  `Document Type` AS document_type,
  `Project Number` AS project_number,
  `Project Url` AS project_url,
  `Product Line` AS product_line,
  `Region` AS region,
  a.`Country` AS country,
  `Project Description` AS project_description,
  `Industry` AS industry,
  `Environmental Category` AS environmental_category,
  `Department` AS department,
  `Status` AS status,
  `Projected Board Date` AS projected_board_date,
  `IFC Approval Date` AS ifc_approval_date,
  `IFC Signed Date` AS ifc_signed_date,
  `IFC Invested Date` AS ifc_invested_date,
  `IFC investment for Risk Management(Million - USD)` AS ifc_investment_for_risk_management_million_usd,
  `IFC investment for Guarantee(Million - USD)` AS ifc_investment_for_guarantee_million_usd,
  `IFC investment for Loan(Million - USD)` AS ifc_investment_for_loan_million_usd,
  `IFC investment for Equity(Million - USD)` AS ifc_investment_for_equity_million_usd,
  `Total IFC investment as approved by Board(Million - USD)` AS total_ifc_investment_as_approved_by_board_million_usd,
  `As of Date` AS as_of_date,
  c.country as wb_country_code,
  c.latitude,
  c.longitude,
  c.name as country_name
FROM apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.synthetic_ifc_projects_5000 a
inner join apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.countries c 
on UPPER(a.country) = UPPER(c.name);

-- COMMAND ----------

  select * from apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.synthetic_ifc_projects_5000_cleaned where country_name = 'Korea'

-- COMMAND ----------

DROP TABLE IF EXISTS apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.ifc_investment_services_projects_cleaned;

CREATE table apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.ifc_investment_services_projects_cleaned as 

SELECT
  `Date Disclosed` AS date_disclosed,
  `Project Name` AS project_name,
  `Document Type` AS document_type,
  `Project Number` AS project_number,
  `Project Url` AS project_url,
  `Product Line` AS product_line,
  `Region` AS region,
  a.`Country` AS country,
  `Project Description` AS project_description,
  `Industry` AS industry,
  `Environmental Category` AS environmental_category,
  `Department` AS department,
  `Status` AS status,
  `Projected Board Date` AS projected_board_date,
  `IFC Approval Date` AS ifc_approval_date,
  `IFC Signed Date` AS ifc_signed_date,
  `IFC Invested Date` AS ifc_invested_date,
  `IFC investment for Risk Management(Million - USD)` AS ifc_investment_for_risk_management_million_usd,
  `IFC investment for Guarantee(Million - USD)` AS ifc_investment_for_guarantee_million_usd,
  `IFC investment for Loan(Million - USD)` AS ifc_investment_for_loan_million_usd,
  `IFC investment for Equity(Million - USD)` AS ifc_investment_for_equity_million_usd,
  `Total IFC investment as approved by Board(Million - USD)` AS total_ifc_investment_as_approved_by_board_million_usd,
  `As of Date` AS as_of_date,
  a.`WB Country Code` as wb_country_code,
  c.country as reference_country_code,
  c.latitude,
  c.longitude,
  c.name as country_name
FROM apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.ifc_investment_services_projects a
inner join apj_databricks_hackathon_2025.apj_quant_masters_genie_hackathon.countries c 
on (a.`WB Country Code`) = UPPER(c.country);

-- COMMAND ----------


