flowchart TB

    %% =========================
    %% DATA SOURCES
    %% =========================
    subgraph SOURCES["Data Sources"]
        YF["Yahoo Finance<br/>SMH ETF"]
        SP["Yahoo Finance<br/>S&P 500"]
        FRED["FRED<br/>New Orders<br/>Inventories<br/>Semiconductor PPI"]
    end

    %% =========================
    %% ORCHESTRATION
    %% =========================
    subgraph ORCHESTRATION["Orchestration"]
        AIRFLOW["Apache Airflow<br/>Docker"]
        
        SMH_DAG["SMH Market Data DAG<br/>Daily 08:00"]
        SP_DAG["S&P 500 Market Data DAG<br/>Daily 08:00"]
        FRED_DAG["FRED Semiconductor DAG<br/>Monthly"]
        
        AIRFLOW --> SMH_DAG
        AIRFLOW --> SP_DAG
        AIRFLOW --> FRED_DAG
    end

    YF --> SMH_DAG
    SP --> SP_DAG
    FRED --> FRED_DAG

    %% =========================
    %% RAW STORAGE
    %% =========================
    subgraph STORAGE["Cloud Storage"]
        PARQUET["Parquet Files"]
        GCS["Google Cloud Storage<br/>Raw Data Lake"]
    end

    SMH_DAG --> PARQUET
    SP_DAG --> PARQUET
    FRED_DAG --> PARQUET

    PARQUET --> GCS

    %% =========================
    %% BIGQUERY RAW
    %% =========================
    subgraph RAW["BigQuery Raw Layer"]
        RAW_TABLES["External Tables<br/><br/>smh_prices<br/>sp500_prices<br/>fred_new_orders<br/>fred_inventories<br/>fred_semiconductor_ppi"]
    end

    GCS --> RAW_TABLES

    %% =========================
    %% DBT
    %% =========================
    subgraph DBT["dbt Cloud Production"]
        DBT_JOB["dbt Cloud<br/>Production Job"]

        STAGING["Staging Models"]
        INTERMEDIATE["Intermediate Models"]
        MART["Mart Models"]

        DBT_JOB --> STAGING
        STAGING --> INTERMEDIATE
        INTERMEDIATE --> MART
    end

    RAW_TABLES --> DBT_JOB

    %% Airflow triggers dbt Cloud
    AIRFLOW -. "trigger production job" .-> DBT_JOB

    %% =========================
    %% ANALYTICS
    %% =========================
    subgraph ANALYTICS["Analytics & BI"]
        BIGQUERY_MART["BigQuery<br/>Analytics / Mart Layer"]
        LOOKER["Looker Studio<br/>Dashboard"]
    end

    MART --> BIGQUERY_MART
    BIGQUERY_MART --> LOOKER

    %% =========================
    %% CI
    %% =========================
    subgraph CI["CI — Continuous Integration"]
        GITHUB["GitHub Repository"]
        PR["Pull Request"]
        ACTIONS["GitHub Actions"]
        PARSE["dbt parse"]
        VALIDATION["CI Validation"]
        
        GITHUB --> PR
        PR --> ACTIONS
        ACTIONS --> PARSE
        PARSE --> VALIDATION
    end

    %% =========================
    %% CD / PRODUCTION
    %% =========================
    subgraph CD["CD / Automated Production Pipeline"]
        SCHEDULE["Scheduled Airflow DAGs"]
        TRIGGER["DbtCloudRunJobOperator"]
        PROD["dbt Cloud Production Job"]
        
        SCHEDULE --> TRIGGER
        TRIGGER --> PROD
    end

    AIRFLOW -. "scheduled execution" .-> SCHEDULE
    TRIGGER -. "trigger" .-> DBT_JOB

    %% =========================
    %% INFRASTRUCTURE
    %% =========================
    subgraph INFRA["Infrastructure as Code"]
        TERRAFORM["Terraform"]
        GCP["Google Cloud Infrastructure"]
        
        TERRAFORM --> GCP
    end

    GCP -. "provisions" .-> GCS
    GCP -. "provisions" .-> RAW_TABLES