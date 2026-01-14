# Databricks notebook source
version: 1.1

source: `apj_databricks_hackathon_2025`.`apj_quant_master's_genie_hackathon`.`synthetic_ifc_projects_5000`

dimensions:
  - name: date_disclosed
    expr: "`Date Disclosed`"
    display_name: Date Disclosed
    format:
      type: date
      date_format: year_month_day
      leading_zeros: false
    comment: The date when the project information was made public, which is crucial for tracking project timelines and transparency.

  - name: project_name
    expr: "`Project Name`"
    display_name: Project Name
    comment: The official name of the project, which helps in identifying and referencing the project in discussions and reports.

  - name: document_type
    expr: "`Document Type`"
    display_name: Document Type
    comment: Indicates the type of document associated with the project, providing context on the nature of the information available.

  - name: project_number
    expr: "`Project Number`"
    display_name: Project Number
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
      abbreviation: none
      hide_group_separator: false
    comment: A unique identifier assigned to each project, facilitating easy tracking and management of project-related data.

  - name: project_url
    expr: "`Project Url`"
    display_name: Project Url
    comment: A link to the project's webpage or relevant documentation, allowing for quick access to more detailed information.

  - name: product_line
    expr: "`Product Line`"
    display_name: Product Line
    comment: Specifies the category of financial products associated with the project, which can aid in understanding the project's financial structure.

  - name: region
    expr: "`Region`"
    display_name: Region
    comment: Denotes the geographical region where the project is located, useful for regional analysis and investment distribution.

  - name: country
    expr: "`Country`"
    display_name: Country
    comment: Identifies the specific country in which the project is implemented, essential for country-specific assessments and reporting.

  - name: project_description
    expr: "`Project Description`"
    display_name: Project Description
    comment: A brief overview of the project's objectives and scope, providing insight into its purpose and expected outcomes.

  - name: industry
    expr: "`Industry`"
    display_name: Industry
    comment: Categorizes the project within a specific industry, which helps in analyzing trends and impacts across different sectors.

  - name: environmental_category
    expr: "`Environmental Category`"
    display_name: Environmental Category
    comment: Classifies the project based on its environmental impact, important for assessing sustainability and compliance with regulations.

  - name: department
    expr: "`Department`"
    display_name: Department
    comment: Indicates the department responsible for the project, which can assist in understanding organizational structure and accountability.

  - name: status
    expr: "`Status`"
    display_name: Status
    comment: Reflects the current state of the project, such as active, completed, or on hold, which is vital for tracking progress.

  - name: projected_board_date
    expr: "`Projected Board Date`"
    display_name: Projected Board Date
    format:
      type: date
      date_format: year_month_day
      leading_zeros: false
    comment: The anticipated date for the project to be presented to the board for approval, important for planning and scheduling.

  - name: ifc_approval_date
    expr: "`IFC Approval Date`"
    display_name: IFC Approval Date
    format:
      type: date
      date_format: year_month_day
      leading_zeros: false
    comment: The date when the project received official approval from the International Finance Corporation, marking a key milestone.

  - name: ifc_signed_date
    expr: "`IFC Signed Date`"
    display_name: IFC Signed Date
    format:
      type: date
      date_format: year_month_day
      leading_zeros: false
    comment: The date when the project agreement was formally signed, indicating the commitment to proceed with the project.

  - name: ifc_invested_date
    expr: "`IFC Invested Date`"
    display_name: IFC Invested Date
    format:
      type: date
      date_format: year_month_day
      leading_zeros: false
    comment: The date when the investment from the IFC was made, which is crucial for financial tracking and reporting.

  - name: wb_country_code
    expr: "`WB Country Code`"
    display_name: WB Country Code
    comment: The World Bank's country code for the project location, useful for standardizing country references in reports.

  - name: as_of_date
    expr: "`As of Date`"
    display_name: As of Date
    format:
      type: date
      date_format: year_month_day
      leading_zeros: false
    comment: The date up to which the data is current, important for ensuring the relevance and accuracy of the information presented.

measures:
  - name: ifc_investment_for_risk_management_million_usd
    expr: SUM(`IFC investment for Risk Management(Million - USD)`)
    display_name: IFC Investment for Risk Management (Million USD)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: none
      hide_group_separator: false
    comment: The amount allocated for risk management purposes, providing insight into the project's financial strategy.

  - name: ifc_investment_for_guarantee_million_usd
    expr: SUM(`IFC investment for Guarantee(Million - USD)`)
    display_name: IFC Investment for Guarantee (Million USD)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: none
      hide_group_separator: false
    comment: The funds designated for guarantees, which can help in understanding the project's risk mitigation measures.

  - name: ifc_investment_for_loan_million_usd
    expr: SUM(`IFC investment for Loan(Million - USD)`)
    display_name: IFC Investment for Loan (Million USD)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: none
      hide_group_separator: false
    comment: The total investment provided in the form of loans, essential for analyzing the financial structure of the project.

  - name: ifc_investment_for_equity_million_usd
    expr: SUM(`IFC investment for Equity(Million - USD)`)
    display_name: IFC Investment for Equity (Million USD)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: none
      hide_group_separator: false
    comment: The amount invested as equity, which is important for assessing the ownership and financial stakes in the project.

  - name: total_ifc_investment_approved_by_board_million_usd
    expr: SUM(`Total IFC investment as approved by Board(Million - USD)`)
    display_name: Total IFC Investment as Approved by Board (Million USD)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: none
      hide_group_separator: false
    comment: The total investment amount approved by the board, providing a comprehensive view of the financial commitment to the project.
