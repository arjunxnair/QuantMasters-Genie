-- Databricks notebook source
-- MAGIC %md
-- MAGIC data sourced from https://financesone.worldbank.org/ifc-investment-services-projects/DS00499 as the data available in the github location was synthetic and most importantly the text didnt make sense and would result in bad analysis by an LLM 

-- COMMAND ----------

create table demo.arjunxnair.ifc_investment_services_projects_cleaned as 

SELECT
  `Date Disclosed` AS date_disclosed,
  `Project Name` AS project_name,
  `Document Type` AS document_type,
  `Project Number` AS project_number,
  `Project Url` AS project_url,
  `Product Line` AS product_line,
  `Company Name` AS company_name,
  `Country` AS country,
  `IFC Country Code` AS ifc_country_code,
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
  `WB Country Code` AS wb_country_code,
  `As of Date` AS as_of_date
FROM demo.arjunxnair.ifc_investment_services_projects

-- COMMAND ----------

select * from demo.arjunxnair.ifc_investment_services_projects_cleaned

-- COMMAND ----------

select country, sum(total_ifc_investment_as_approved_by_board_million_usd) from demo.arjunxnair.ifc_investment_services_projects_cleaned group by country

-- COMMAND ----------



-- COMMAND ----------

CREATE VIEW demo.arjunxnair.ifc_investment_services_projects_cleaned_view AS

SELECT *,
DATEDIFF(`ifc_signed_date`, `ifc_approval_date`) as days_between_approval_and_signing,
DATEDIFF(`ifc_invested_date`, `ifc_signed_date`) as days_between_signing_and_investment,
DATEDIFF(`ifc_invested_date`, `ifc_approval_date`) as days_between_approval_and_investment,
CASE
    WHEN
      `country` ILIKE '%East Asia%'
      OR `country` ILIKE '%Pacific%'
      OR `country` IN (
        'China',
        'Viet Nam',
        'Indonesia',
        'Thailand',
        'Philippines',
        'Malaysia',
        'Cambodia',
        'Lao PDR',
        'Mongolia',
        'Myanmar',
        'Timor-Leste'
      )
    THEN
      'East Asia & Pacific'
    WHEN
      `country` ILIKE '%Europe%'
      OR `country` ILIKE '%Central Asia%'
      OR `country` IN (
        'Romania',
        'Turkey',
        'Turkiye',
        'Poland',
        'Ukraine',
        'Kazakhstan',
        'Uzbekistan',
        'Georgia',
        'Serbia',
        'Croatia',
        'Bulgaria',
        'Armenia',
        'Azerbaijan',
        'Kyrgyz Republic',
        'Tajikistan',
        'Albania',
        'Bosnia and Herzegovina',
        'North Macedonia',
        'Kosovo',
        'Moldova',
        'Slovak Republic',
        'Slovenia',
        'Czech Republic',
        'Hungary',
        'Estonia',
        'Latvia',
        'Lithuania',
        'Belarus',
        'Russia'
      )
    THEN
      'Europe & Central Asia'
    WHEN
      `country` ILIKE '%Latin America%'
      OR `country` ILIKE '%Caribbean%'
      OR `country` IN (
        'Brazil',
        'Mexico',
        'Colombia',
        'Argentina',
        'Chile',
        'Peru',
        'Ecuador',
        'Guatemala',
        'Honduras',
        'El Salvador',
        'Costa Rica',
        'Panama',
        'Nicaragua',
        'Bolivia',
        'Paraguay',
        'Uruguay',
        'Venezuela',
        'Dominican Republic',
        'Jamaica',
        'Trinidad and Tobago',
        'Barbados',
        'Bahamas',
        'Haiti',
        'Cuba',
        'Puerto Rico',
        'Guyana',
        'Suriname',
        'Belize',
        'St. Lucia',
        'Grenada',
        'St. Vincent and the Grenadines',
        'Antigua and Barbuda',
        'Dominica',
        'St. Kitts and Nevis'
      )
    THEN
      'Latin America & Caribbean'
    WHEN
      `country` ILIKE '%Middle East%'
      OR `country` ILIKE '%North Africa%'
      OR `country` IN (
        'Egypt, Arab Republic of',
        'Morocco',
        'Tunisia',
        'Algeria',
        'Libya',
        'Jordan',
        'Lebanon',
        'Syrian Arab Republic',
        'Iraq',
        'Iran',
        'West Bank and Gaza',
        'Yemen, Republic of',
        'Djibouti'
      )
    THEN
      'Middle East & North Africa (MENA)'
    WHEN `country` IN ('United States', 'Canada', 'Bermuda') THEN 'North America'
    WHEN
      `country` ILIKE '%South Asia%'
      OR `country` IN (
        'India', 'Bangladesh', 'Pakistan', 'Sri Lanka', 'Nepal', 'Bhutan', 'Maldives', 'Afghanistan'
      )
    THEN
      'South Asia'
    WHEN
      `country` ILIKE '%Sub-Saharan Africa%'
      OR `country` ILIKE '%Africa%'
      OR `country` IN (
        'Nigeria',
        'South Africa',
        'Kenya',
        'Ghana',
        'Ethiopia',
        'Tanzania',
        'Uganda',
        'Angola',
        'Mozambique',
        'Zambia',
        'Senegal',
        'Cote d\'Ivoire',
        'Cameroon',
        'Democratic Republic of the Congo',
        'Sudan',
        'Rwanda',
        'Burkina Faso',
        'Mali',
        'Niger',
        'Malawi',
        'Botswana',
        'Namibia',
        'Gabon',
        'Congo, Republic of',
        'Sierra Leone',
        'Liberia',
        'Mauritius',
        'Madagascar',
        'Chad',
        'Benin',
        'Togo',
        'Guinea',
        'Burundi',
        'Lesotho',
        'Swaziland',
        'Gambia',
        'Cape Verde',
        'Sao Tome and Principe',
        'Comoros',
        'Seychelles',
        'Eritrea',
        'Central African Republic',
        'Equatorial Guinea',
        'Guinea-Bissau',
        'Somalia',
        'South Sudan'
      )
    THEN
      'Sub-Saharan Africa'
    ELSE 'Other/World/Unclassified'
  END AS world_bank_region
FROM demo.arjunxnair.ifc_investment_services_projects_cleaned

-- COMMAND ----------

-- Total IFC loan investment by country for the last 10 years.

SELECT
  `country` AS country,
  SUM(`ifc_investment_for_loan_million_usd`) AS total_loan_investment_musd
FROM
  demo.arjunxnair.`ifc_investment_services_projects_cleaned`
WHERE
  `date_disclosed` BETWEEN date_add(YEAR, -10, CURRENT_DATE) AND CURRENT_DATE
  AND `ifc_investment_for_loan_million_usd` IS NOT NULL
  AND `country` IS NOT NULL
GROUP BY
  `country`
ORDER BY
  total_loan_investment_musd DESC

-- COMMAND ----------

-- Top projects by Total IFC investment as approved.

SELECT
  `project_name`,
  `country`,
  `total_ifc_investment_as_approved_by_board_million_usd`
FROM
  demo.arjunxnair.`ifc_investment_services_projects_cleaned`
WHERE
  `total_ifc_investment_as_approved_by_board_million_usd` IS NOT NULL
ORDER BY
  `total_ifc_investment_as_approved_by_board_million_usd` DESC

-- COMMAND ----------

-- Compare Equity vs Loan investment across industries.
SELECT
  `industry`,
  SUM(`ifc_investment_for_equity_million_usd`) AS total_equity_investment_musd,
  SUM(`ifc_investment_for_loan_million_usd`) AS total_loan_investment_musd,
  (
    SUM(`ifc_investment_for_equity_million_usd`) + SUM(`ifc_investment_for_loan_million_usd`)
  ) AS total_investment_musd,
  try_divide(
    100 * SUM(`ifc_investment_for_equity_million_usd`),
    SUM(`ifc_investment_for_equity_million_usd`) + SUM(`ifc_investment_for_loan_million_usd`)
  ) AS pct_equity_investment,
  try_divide(
    100 * SUM(`ifc_investment_for_loan_million_usd`),
    SUM(`ifc_investment_for_equity_million_usd`) + SUM(`ifc_investment_for_loan_million_usd`)
  ) AS pct_loan_investment
FROM
  demo.arjunxnair.`ifc_investment_services_projects_cleaned`
WHERE
  `industry` IS NOT NULL
GROUP BY
  `industry`
ORDER BY
  total_investment_musd DESC

-- COMMAND ----------

SELECT 
    percentile(days_between_approval_and_investment, 0.25) AS q1,
    percentile(days_between_approval_and_investment, 0.75) AS q3,
    percentile(days_between_approval_and_investment, 0.5) AS median, -- Optional: for the median line
    industry
FROM 
     demo.arjunxnair.ifc_investment_services_projects_cleaned_view
GROUP BY 
    industry;

-- COMMAND ----------


