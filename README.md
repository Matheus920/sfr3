# SFR3: General Ledger ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-blue.svg)
![Pydantic](https://img.shields.io/badge/Pydantic-Data_Validation-green.svg)

## Table of Contents
- [Introduction](#introduction)
- [Data Model and Warehouse](#data-model-and-warehouse)
- [ETL Process](#etl-process)
  - [Extract](#extract)
  - [Transform](#transform)
  - [Load](#load)
- [Future Orchestration](#future-orchestration)
- [ML & LLM Interactions](#ml--llm-interactions)
- [AI Tools Used During Development](#ai-tools-used-during-development)

## Introduction

This document describes the implementation of the technical assessment for the SFR3 Data Engineer position, using the General Ledger Accounts and Transactions use case. This document is divided into the following sections:

1. **Data Model and Warehouse**: Description of the data model and warehouse structure used to store the General Ledger Accounts and Transactions.
2. **ETL Process**: Description of the ETL process used to load the data into the data model.
3. **Future Orchestration**: Description of the future orchestration process to automate the ETL process.
4. **ML & LLM Interactions**: Description of possible interactions between the data model and the Machine Learning models or LLM agents.
5. **AI Tools during development**: Description of the AI tools used during the development of the ETL process.


## Data Model and Warehouse

### Data Sources and Relationships

The data for this project comes from two primary sources:

1. **General Ledger Accounts**
   - Base entity storing account information
   - Hierarchical structure (main accounts and sub-accounts)
   - Contains account metadata like type, classification, and status
   - Analysis revealed a single-level hierarchy (sub-accounts don't have their own sub-accounts)

2. **Transactions**
   - Financial transactions related to accounts
   - Each transaction can relate to multiple accounts through "lines"
   - Many-to-many relationship with accounts (each transaction can involve multiple accounts)

The requirements initially suggested a direct account_id relationship between transactions and accounts. However, the actual structure revealed that transactions relate to accounts through a "lines" attribute, where each line references an account. This creates a many-to-many relationship between transactions and accounts.

While normalizing the "Lines" entity into its own table would be the theoretically correct approach, I chose to simplify the model by:
- Keeping the lines data as a VARIANT JSON type
- Creating a bridge table (account_transactions) to represent the many-to-many relationship

### Idempotent Processing Architecture

A key requirement was making the ETL process idempotent (able to run multiple times without creating duplicates). To achieve this:

- Created separate staging and production schemas
- Implemented a two-phase load process:
  1. Load into staging tables (allowing duplicates during processing)
  2. Merge from staging to final tables with duplicate handling
- Added run_id tracking for lineage and debugging
- Maintained historical data in staging for troubleshooting (could be periodically cleaned)

To accomplish the above, this is the DDL of the tables that were created in the data warehouse:

```sql
create database sfr3;

create schema general_ledger_staging;

create or replace table account(
    id int primary key,
    account_number varchar(255),
    name varchar(255),
    description varchar(255),
    type varchar(255),
    sub_type varchar(255),
    is_default_gl_account boolean,
    default_account_name varchar(255),
    is_contra_account boolean,
    is_bank_account boolean,
    cash_flow_classification varchar(255),
    exclude_from_cash_balances boolean,
    is_active boolean,
    parent_gl_account_id int,
    run_id varchar(255),
    inserted_at timestamp default current_timestamp,
    foreign key (parent_gl_account_id) references account(id)
);

create or replace table transaction(
    id int primary key,
    date date,
    transaction_type varchar(255),
    total_amount numeric(38, 2),
    check_number varchar(255),
    unit_agreement variant,
    unit_id int,
    unit_number varchar(255),
    payment_detail variant,
    deposit_details variant,
    journal_memo varchar(255),
    lines variant,
    last_updated_date_time timestamp,
    run_id varchar(255),
    inserted_at timestamp default current_timestamp
);


create or replace table account_transactions(
    account_id int,
    transaction_id int,
    run_id varchar(255),
    inserted_at timestamp default current_timestamp,
    primary key(account_id, transaction_id),
    foreign key (account_id) references account(id),
    foreign key (transaction_id) references transaction(id)
);

create schema general_ledger;

create or replace table account(
    id int primary key,
    account_number varchar(255),
    name varchar(255),
    description varchar(255),
    type varchar(255),
    sub_type varchar(255),
    is_default_gl_account boolean,
    default_account_name varchar(255),
    is_contra_account boolean,
    is_bank_account boolean,
    cash_flow_classification varchar(255),
    exclude_from_cash_balances boolean,
    is_active boolean,
    parent_gl_account_id int,
    inserted_at timestamp default current_timestamp,
    foreign key (parent_gl_account_id) references account(id)
);

create or replace table transaction(
    id int primary key,
    date date,
    transaction_type varchar(255),
    total_amount numeric(38, 2),
    check_number varchar(255),
    unit_agreement variant,
    unit_id int,
    unit_number varchar(255),
    payment_detail variant,
    deposit_details variant,
    journal_memo varchar(255),
    lines variant,
    last_updated_date_time timestamp,
    inserted_at timestamp default current_timestamp
);


create or replace table account_transactions(
    account_id int,
    transaction_id int,
    inserted_at timestamp default current_timestamp,
    primary key(account_id, transaction_id),
    foreign key (account_id) references account(id),
    foreign key (transaction_id) references transaction(id)
);
```

As it is possible to see, many fields were kept as VARIANT where normally they would be normalized into their own tables. This was done to simplify the data model and the ETL process.

## ETL Process

The ETL Process was developed using Python and Snowflake. Before talking about the implementation, I would like to mention that I would have created this as a ELT process, where the data would be loaded as raw as possible into the data warehouse and transformed into final models using dbt. What I don't like about performing the transformations in Python is that I believe the nested structures and normalization can be confusing and not well performative in Python. However, I followed the requirements and implemented the ETL process in Python.

Talking about the implementation, the ETL process was divided into three main modules: extract, transform and load. Let's explore each one of them:

### Extract

The extract module serves as the first phase of the ETL pipeline, responsible for retrieving data from source systems. 

#### Data Sources & Format

For this implementation:
- Data comes from sample JSON files (simulating API responses)
- Two primary data entities:
  - General Ledger Accounts
  - Transactions

#### Implementation Details

The extraction process involved:

1. **JSON Parsing & Validation with Pydantic**
   - Used Pydantic to create strongly-typed models
   - Validated incoming JSON against schema definitions
   - Converted raw JSON to structured Python objects
   - This provided type safety and self-documenting models for the transform phase

2. **Simulated API Interaction**
   - Implemented an account-based transaction retrieval system
   - Fetched transactions for each account individually
   - Simulated real-world API interactions where data might be paginated or restricted

3. **Design Considerations**
   - While the API seemed capable of accepting multiple account IDs at once, I followed the 
     proposed implementation pattern of one-account-at-a-time retrieval
   - This approach would be more resilient in scenarios with rate limits or large data volumes
   - Created extraction functions that could be easily modified to handle real API endpoints

### Transform

The transform module functions as the central component of the ETL pipeline, converting extracted data into the target data model format. This phase handles all data normalization, relationship mapping, and structural transformations needed for the warehouse schema.

#### Key Transformation Operations

1. **Account Hierarchy Flattening**
   - Converted hierarchical parent-child account structures into flat records
   - Preserved parent-child relationships through foreign key references
   - Handled special cases like default accounts and contra accounts
   - Generated complete account records ready for loading

2. **Many-to-Many Relationship Mapping**
   - Extracted account references from transaction line items
   - Created bridge table records linking transactions to accounts
   - Ensured proper relationship cardinality
   - Generated the junction records for the account_transactions table

3. **Data Type Transformations**
   - Converted JSON date strings to proper date objects
   - Handled numeric formatting for financial values
   - Preserved complex nested structures as VARIANT type data
   - Applied appropriate type casting for all fields

#### Implementation Approach

The transformation logic leveraged the Pydantic models created in the extract phase:

- **Object-Oriented Approach**: Used Python objects to represent entities
- **Type Safety**: Pydantic's validation ensured data integrity during transformation
- **Modularity**: Separated transformation logic into focused functions for maintainability

While pandas could have been an alternative for transformation tasks, I opted for standard Python iteration since:
- The data volume was relatively small
- Transformations were straightforward
- Pydantic models provided sufficient structure
- This approach maintained consistency with the extract phase

### Load

The load module serves as the final phase of the ETL pipeline, responsible for persisting transformed data into the Snowflake data warehouse. This critical stage implements the idempotent loading pattern required by the specifications.

#### Two-Phase Loading Strategy

I implemented a two-phase loading approach:

1. **Staging Phase**
   - Transformed data first converted to CSV format for efficient bulk loading
   - Data loaded into staging tables using Snowflake Python Connector
   - Run ID appended to each record for lineage tracking
   - Staging tables accept duplicate records during processing

2. **Production Phase**
   - Data moved from staging to production tables using MERGE operations
   - MERGE statements handle duplicate detection and prevention
   - Only new or changed records inserted into final tables
   - Primary and foreign key constraints enforced at this stage

#### Implementation Details

The loading process included several key components:

1. **Data Format Conversion**
   - Python objects serialized to CSV format
   - CSV format chosen for compatibility with Snowflake's bulk loading capabilities
   - Field mappings carefully defined to ensure proper column alignment

2. **Snowflake Connection Management**
   - Error handling with appropriate rollback mechanisms
   - Transactions used to ensure data consistency

3. **Idempotent Processing**
   - SQL MERGE statements designed for idempotent operations
   - Merge criteria based on primary keys and business identifiers
   - This approach enables the ETL process to be safely re-run without creating duplicates
   - Records tracked with run_id for debugging and lineage purposes

## Future Orchestration

The ETL process implemented here would benefit from a platform like Apache Airflow. Airflow could be used to automate the ETL process by:

- Dividing the process into discrete, monitored tasks
- Executing tasks in a specific order with dependencies
- Managing retries and error handling
- Providing visibility into pipeline execution

The scheduling frequency would depend on two main factors:
1. **Data update frequency** - How often source data changes
2. **Processing time requirements** - How long the ETL process takes to run

The ETL process could be organized into the following DAG structure:

```
extract_accounts → transform_accounts → extract_transactions → transform_transactions → load_data
```

### Alternative Approach: ELT with dbt

If this process was implemented following an ELT (Extract, Load, Transform) approach instead:
- Airflow might not be necessary
- Transformations would be done using dbt directly in the data warehouse
- dbt could be scheduled to run at specific intervals
- A simple AWS Lambda function could trigger the extraction module
- The dbt models could then be triggered by the Lambda function when extraction completes

## ML & LLM Interactions

I will focus on the LLM Interactions since that's the area I have more experience with. This data can be used to augment an AI Agent responsible for answering questions about the General Ledger Accounts and Transactions.

### LLM as a Financial Data Assistant

The AI Agent could be trained to answer questions like:
- "What is the total amount of transactions for a specific account?"
- "What is the total amount of transactions for account X in date range Y?"
- "What is the total amount of transactions with transaction type Z?"

I would focus on augmenting the LLM by creating a tool that can access the Snowflake database to obtain data in real-time, rather than fine-tuning the LLM with static data. This approach allows the LLM to answer questions about the General Ledger without requiring human intervention to access the data.

### LLM Integration Best Practices

Based on my experience, here are key learnings for financial data LLM integration:

1. **Simplified Data Interface**
   - Use a single denormalized table as the interface between the LLM and the data
   - LLMs struggle with complex joins and multi-table relationships
   
2. **Clear Data Definition**
   - Provide explicit column definitions and example values
   - Include data types, valid ranges, and business meaning for each field
   
3. **Comprehensive Logging**
   - Implement session logging that captures both queries and responses
   - Enable session replay to understand reasoning and improve system
   
4. **Versioned Prompt Engineering**
   - Implement version control for prompts
   - Enable easy rollbacks when prompt changes cause performance regressions

### Machine Learning Use Cases

Beyond LLM applications, this data could power several ML models:

1. **Anomaly Detection**
   - **Use Case**: Predict and flag transactions that deviate from normal patterns
   - **Predictive Variables**: Transaction amount deviations, frequency, unusual combinations of GL account types, text sentiment changes
   - **Potential Implementation**: Isolation forests or autoencoders for unsupervised anomaly detection

2. **Cash Flow Forecasting**
   - **Use Case**: Predict future revenues or cash flows based on historical trends
   - **Predictive Variables**: Transaction volumes, seasonal patterns, rolling averages
   - **Potential Implementation**: Time series models (ARIMA, Prophet) or recurrent neural networks

3. **Account Segmentation**
   - **Use Case**: Segment accounts based on behavior or risk profiles
   - **Predictive Variables**: Transaction frequency, amounts, type distributions
   - **Potential Implementation**: K-means clustering or hierarchical clustering models

4. **Risk Assessment**
   - **Use Case**: Estimate risk of disputes, defaults, or operational issues
   - **Predictive Variables**: Historical anomalies, adjustment frequencies, trend analysis
   - **Potential Implementation**: Gradient boosting models with feature importance analysis

## AI Tools Used During Development

During the development of this ETL pipeline, I leveraged AI tools to enhance productivity while maintaining control over core architecture and business logic decisions. Here's how I incorporated AI assistance:

### Claude 3.7 (via Cline)

I consider Claude 3.7 to be the best generative code tool for my specific use cases. While I don't use it to design core business logic or project structure, it excels at helping with repetitive tasks:

- **Documentation Generation**: Creating comprehensive docstrings that follow consistent standards
- **Test Creation**: Generating unit tests with appropriate edge cases and assertions
- **Schema Conversion**: Translating complex JSON schemas to Pydantic models
- **Logging Enhancement**: Implementing consistent, informative logging patterns

### GPT-3o-mini-high

I used GPT-3o-mini-high primarily as a code review and brainstorming partner:

- **Code Refactoring**: Getting suggestions for improving code structure and readability
- **Architecture Discussions**: Exploring alternative approaches to problem-solving
- **Idea Validation**: Testing my implementation ideas against different perspectives
- **Alternative Solutions**: Identifying design patterns I might not have considered

### AI Usage Philosophy

For this project, I maintained a balanced approach where:
- Core business logic and architectural decisions remained human-driven
- AI tools were used to accelerate implementation of standard patterns
- All AI-generated code was reviewed and validated before incorporation
- AI served as a collaborative tool rather than a replacement for engineering judgment
