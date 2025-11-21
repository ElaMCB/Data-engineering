# ETL Pipeline Framework - Visualizations & Architecture Diagrams

This document contains visual representations of the ETL pipeline framework architecture, data flows, and system components.

---

## 1. High-Level Architecture Overview

```mermaid
graph TB
    subgraph "Data Sources"
        S3[S3 Raw Data<br/>Parquet/CSV/JSON]
        RDS[RDS Databases<br/>PostgreSQL/MySQL]
        API[External APIs<br/>REST/SOAP]
    end
    
    subgraph "ETL Framework"
        subgraph "Extract Layer"
            E1[S3 Extractor]
            E2[RDS Extractor]
        end
        
        subgraph "Transform Layer"
            T1[Data Transformer<br/>Cleaning & Validation]
            T2[Business Rules<br/>Enrichment]
        end
        
        subgraph "Load Layer"
            L1[Parquet Loader]
            L2[Delta Lake Writer]
        end
    end
    
    subgraph "Supporting Services"
        CDC[CDC Handler<br/>Incremental Loading]
        COST[Cost Optimizer<br/>EMR Configuration]
        SPARK[Spark Utils<br/>Session Management]
    end
    
    subgraph "Target Storage"
        S3OUT[S3 Processed Data<br/>Partitioned Parquet]
        DELTA[Delta Lake Tables<br/>Unity Catalog]
    end
    
    subgraph "Monitoring & QA"
        MON[CloudWatch<br/>Monitoring]
        DQ[Great Expectations<br/>Data Quality]
        ALERT[Alerting<br/>PagerDuty/Slack]
    end
    
    S3 --> E1
    RDS --> E2
    API --> E1
    
    E1 --> T1
    E2 --> T1
    T1 --> T2
    T2 --> L1
    T2 --> L2
    
    CDC --> T1
    COST --> SPARK
    SPARK --> T1
    
    L1 --> S3OUT
    L2 --> DELTA
    
    T1 --> DQ
    T2 --> DQ
    L1 --> MON
    MON --> ALERT
    
    style S3 fill:#e1f5ff
    style RDS fill:#e1f5ff
    style API fill:#e1f5ff
    style S3OUT fill:#d4edda
    style DELTA fill:#d4edda
    style MON fill:#fff3cd
    style DQ fill:#fff3cd
```

---

## 2. ETL Pipeline Execution Flow

```mermaid
sequenceDiagram
    participant User
    participant Pipeline as ETL Pipeline
    participant Extractor
    participant Transformer
    participant Loader
    participant CDC as CDC Handler
    participant Spark
    participant S3 as S3 Storage
    
    User->>Pipeline: Execute Pipeline
    Pipeline->>Pipeline: Validate Configuration
    
    Note over Pipeline: EXTRACT PHASE
    Pipeline->>Extractor: extract()
    Extractor->>Spark: Read from S3/RDS
    Spark->>S3: Load Data
    S3-->>Spark: Return Data
    Spark-->>Extractor: DataFrame
    Extractor-->>Pipeline: Raw Data
    
    Note over Pipeline: TRANSFORM PHASE
    Pipeline->>Transformer: transform(data)
    Transformer->>Transformer: Apply Column Mappings
    Transformer->>Transformer: Apply Transformations
    Transformer->>Transformer: Data Quality Validation
    Transformer->>Transformer: Data Cleaning
    Transformer-->>Pipeline: Transformed Data
    
    Note over Pipeline: LOAD PHASE
    Pipeline->>Loader: load(data)
    alt Incremental Load
        Loader->>CDC: Check for Updates
        CDC->>S3: Read Last Timestamp
        CDC-->>Loader: Incremental Data
    end
    Loader->>Spark: Write to S3
    Spark->>S3: Write Parquet Files
    S3-->>Spark: Write Confirmation
    Spark-->>Loader: Success
    Loader-->>Pipeline: Load Complete
    
    Pipeline-->>User: Execution Results
```

---

## 3. Oil & Gas Ticket Transaction Pipeline - Data Flow

```mermaid
graph LR
    subgraph "Source Systems"
        SOURCE1[Carrier Systems<br/>Transaction Files]
        SOURCE2[Port Management<br/>Vessel Data]
        SOURCE3[Customer Systems<br/>Contract Data]
    end
    
    subgraph "Raw Data Lake"
        RAW[S3 Raw Zone<br/>s3://oil-gas-data-lake/raw-transactions/]
    end
    
    subgraph "ETL Processing"
        EXTRACT[Extract<br/>10-15M records/day]
        TRANSFORM[Transform<br/>- Standardize fuel types<br/>- Calculate amounts<br/>- Validate data quality]
        LOAD[Load<br/>Partitioned by date/fuel_type]
    end
    
    subgraph "Processed Data Lake"
        PROCESSED[S3 Processed Zone<br/>s3://oil-gas-data-lake/processed-transactions/]
        PARTITION[Partitioned by:<br/>year/month/day/fuel_type]
    end
    
    subgraph "Consumers"
        ANALYTICS[Analytics Team<br/>Tableau/Looker]
        DATA_SCIENCE[Data Science<br/>ML Models]
        REPORTING[Business Reports<br/>Executive Dashboards]
    end
    
    SOURCE1 --> RAW
    SOURCE2 --> RAW
    SOURCE3 --> RAW
    
    RAW --> EXTRACT
    EXTRACT --> TRANSFORM
    TRANSFORM --> LOAD
    LOAD --> PROCESSED
    PROCESSED --> PARTITION
    
    PARTITION --> ANALYTICS
    PARTITION --> DATA_SCIENCE
    PARTITION --> REPORTING
    
    style RAW fill:#ffcccc
    style PROCESSED fill:#ccffcc
    style EXTRACT fill:#cce5ff
    style TRANSFORM fill:#ffffcc
    style LOAD fill:#ffccff
```

---

## 4. Incremental Loading with CDC (Change Data Capture)

```mermaid
graph TD
    START([Pipeline Start]) --> CHECK{Last Run<br/>Timestamp?}
    
    CHECK -->|Yes| GET[Get Last Processed<br/>Timestamp from Target]
    CHECK -->|No| LOOKBACK[Use Lookback Window<br/>24 hours]
    
    GET --> EXTRACT[Extract Incremental Data<br/>WHERE updated_at > last_timestamp]
    LOOKBACK --> EXTRACT
    
    EXTRACT --> COUNT{Records<br/>Found?}
    
    COUNT -->|No| END1([No Changes<br/>Pipeline Complete])
    COUNT -->|Yes| TRANSFORM[Transform<br/>Incremental Data]
    
    TRANSFORM --> READ[Read Existing<br/>Target Data]
    
    READ --> MERGE[Merge Logic:<br/>1. Remove old records with same keys<br/>2. Append new/updated records]
    
    MERGE --> WRITE[Write Merged<br/>Data to Target]
    
    WRITE --> UPDATE[Update Last<br/>Processed Timestamp]
    
    UPDATE --> END2([Pipeline Complete<br/>Records Updated])
    
    style START fill:#90EE90
    style END1 fill:#FFB6C1
    style END2 fill:#90EE90
    style MERGE fill:#FFD700
```

---

## 5. AWS EMR Cluster Architecture with Cost Optimization

```mermaid
graph TB
    subgraph "EMR Cluster"
        subgraph "Master Node"
            MASTER[Master Node<br/>m5.xlarge<br/>ON-DEMAND]
            LIVY[Livy Server]
            SPARK_MASTER[Spark Master]
        end
        
        subgraph "Core Nodes"
            CORE1[Core Node 1<br/>m5.xlarge<br/>SPOT 70%]
            CORE2[Core Node 2<br/>m5.xlarge<br/>SPOT 70%]
            COREN[Core Node N<br/>Auto-scaled]
        end
        
        subgraph "Task Nodes"
            TASK1[Task Node 1<br/>m5.xlarge<br/>SPOT 70%]
            TASK2[Task Node 2<br/>m5.xlarge<br/>SPOT 70%]
            TASKN[Task Node N<br/>Auto-scaled 2-20]
        end
    end
    
    subgraph "Auto-Scaling"
        MONITOR[CloudWatch<br/>Metrics]
        SCALE_UP[Scale Up<br/>CPU > 75%]
        SCALE_DOWN[Scale Down<br/>CPU < 25%]
    end
    
    subgraph "Storage"
        S3[S3 Data Lake<br/>Raw & Processed]
    end
    
    MASTER --> CORE1
    MASTER --> CORE2
    MASTER --> COREN
    
    CORE1 --> TASK1
    CORE2 --> TASK2
    COREN --> TASKN
    
    MONITOR --> SCALE_UP
    MONITOR --> SCALE_DOWN
    SCALE_UP --> TASKN
    SCALE_DOWN --> TASKN
    
    CORE1 --> S3
    CORE2 --> S3
    TASK1 --> S3
    TASK2 --> S3
    
    style MASTER fill:#FF6B6B
    style CORE1 fill:#4ECDC4
    style CORE2 fill:#4ECDC4
    style TASK1 fill:#95E1D3
    style TASK2 fill:#95E1D3
    style SCALE_UP fill:#FFE66D
    style SCALE_DOWN fill:#FFE66D
```

---

## 6. Data Quality Validation Framework

```mermaid
graph TD
    INPUT[Transformed Data] --> VALIDATE[Data Quality<br/>Validation Engine]
    
    subgraph "Validation Rules"
        NOT_NULL[Not Null Checks<br/>Critical Fields]
        VALUE_RANGE[Value Range<br/>Min/Max Validation]
        ALLOWED_VALUES[Allowed Values<br/>Enum Validation]
        REFERENTIAL[Referential Integrity<br/>Foreign Keys]
        CUSTOM[Custom Business Rules<br/>Complex Logic]
    end
    
    VALIDATE --> NOT_NULL
    VALIDATE --> VALUE_RANGE
    VALIDATE --> ALLOWED_VALUES
    VALIDATE --> REFERENTIAL
    VALIDATE --> CUSTOM
    
    NOT_NULL --> RESULTS{All Rules<br/>Pass?}
    VALUE_RANGE --> RESULTS
    ALLOWED_VALUES --> RESULTS
    REFERENTIAL --> RESULTS
    CUSTOM --> RESULTS
    
    RESULTS -->|Yes| PASS[Pass Validation<br/>Continue to Load]
    RESULTS -->|No| FAIL[Fail Validation<br/>Log Errors]
    
    FAIL --> REPORT[Generate Quality Report<br/>- Failed Records<br/>- Error Details<br/>- Metrics]
    
    REPORT --> ALERT{Error Count<br/>> Threshold?}
    
    ALERT -->|Yes| NOTIFY[Send Alert<br/>PagerDuty/Slack]
    ALERT -->|No| LOG[Log Only]
    
    PASS --> OUTPUT[Validated Data<br/>Ready for Load]
    
    style PASS fill:#90EE90
    style FAIL fill:#FF6B6B
    style NOTIFY fill:#FFD700
```

---

## 7. Pipeline Component Architecture

```mermaid
classDiagram
    class ETLPipeline {
        +Extractor extractor
        +Transformer transformer
        +Loader loader
        +Dict config
        +run() Dict
        +validate() bool
    }
    
    class Extractor {
        <<abstract>>
        +extract() DataFrame
    }
    
    class S3Extractor {
        +source_path: str
        +file_format: str
        +extract() DataFrame
    }
    
    class RDSExtractor {
        +jdbc_url: str
        +table: str
        +extract() DataFrame
    }
    
    class Transformer {
        <<abstract>>
        +transform(DataFrame) DataFrame
    }
    
    class DataTransformer {
        +transformations: List
        +column_mappings: Dict
        +data_quality_rules: List
        +transform(DataFrame) DataFrame
    }
    
    class Loader {
        <<abstract>>
        +load(DataFrame) bool
    }
    
    class ParquetLoader {
        +target_path: str
        +mode: str
        +partition_by: List
        +load(DataFrame) bool
    }
    
    class CDCHandler {
        +key_columns: List
        +timestamp_column: str
        +run_incremental_load() Dict
        +merge_data() bool
    }
    
    class CostOptimizer {
        +use_spot_instances: bool
        +auto_scaling: bool
        +get_emr_config() Dict
        +estimate_cost() Dict
    }
    
    class SparkSessionManager {
        +create_session() SparkSession
        +optimize_for_etl() void
        +read_parquet_optimized() DataFrame
        +write_parquet_optimized() void
    }
    
    ETLPipeline --> Extractor
    ETLPipeline --> Transformer
    ETLPipeline --> Loader
    Extractor <|-- S3Extractor
    Extractor <|-- RDSExtractor
    Transformer <|-- DataTransformer
    Loader <|-- ParquetLoader
    ETLPipeline --> CDCHandler
    ETLPipeline --> CostOptimizer
    ETLPipeline --> SparkSessionManager
```

---

## 8. Oil & Gas Pipeline - End-to-End Process

```mermaid
graph TB
    subgraph "Day 1: Data Ingestion"
        D1A[Carrier Systems<br/>Generate Transactions]
        D1B[Files Written to S3<br/>Raw Zone]
        D1C[Partitioned by<br/>year/month/day]
    end
    
    subgraph "Day 2: ETL Processing"
        D2A[Pipeline Triggered<br/>Daily at 2 AM]
        D2B[Extract: Read Raw Data<br/>10-15M records]
        D2C[Transform:<br/>- Standardize fields<br/>- Calculate amounts<br/>- Validate quality]
        D2D[Load: Write Processed Data<br/>Partitioned Parquet]
    end
    
    subgraph "Day 2: Data Quality"
        D2E[Quality Checks<br/>99.9% pass rate]
        D2F[Generate Reports<br/>Business metrics]
        D2G[Alert on Issues<br/>>100 failures]
    end
    
    subgraph "Day 2: Consumption"
        D2H[Analytics Queries<br/>Tableau/Looker]
        D2I[Data Science<br/>ML Models]
        D2J[Business Reports<br/>Executive dashboards]
    end
    
    D1A --> D1B
    D1B --> D1C
    D1C --> D2A
    D2A --> D2B
    D2B --> D2C
    D2C --> D2D
    D2D --> D2E
    D2E --> D2F
    D2E --> D2G
    D2D --> D2H
    D2D --> D2I
    D2D --> D2J
    
    style D1A fill:#E8F4F8
    style D2C fill:#FFF4E6
    style D2E fill:#E8F5E9
    style D2H fill:#F3E5F5
```

---

## 9. Monitoring & Alerting Architecture

```mermaid
graph LR
    subgraph "Pipeline Execution"
        PIPELINE[ETL Pipeline]
        METRICS[Execution Metrics]
    end
    
    subgraph "Monitoring Stack"
        CW[CloudWatch<br/>Metrics & Logs]
        GRAFANA[Grafana<br/>Dashboards]
        CUSTOM[Custom Metrics<br/>Collector]
    end
    
    subgraph "Alerting"
        RULES[Alert Rules<br/>Thresholds]
        PAGERDUTY[PagerDuty<br/>On-Call]
        SLACK[Slack<br/>Notifications]
        EMAIL[Email<br/>Reports]
    end
    
    subgraph "Data Quality"
        DQ_METRICS[Quality Metrics<br/>Pass/Fail Rates]
        DQ_DASHBOARD[Quality Dashboard]
        DQ_ALERTS[Quality Alerts]
    end
    
    PIPELINE --> METRICS
    METRICS --> CW
    METRICS --> CUSTOM
    
    CW --> GRAFANA
    CUSTOM --> GRAFANA
    
    CW --> RULES
    RULES --> PAGERDUTY
    RULES --> SLACK
    RULES --> EMAIL
    
    PIPELINE --> DQ_METRICS
    DQ_METRICS --> DQ_DASHBOARD
    DQ_METRICS --> DQ_ALERTS
    DQ_ALERTS --> SLACK
    
    style PIPELINE fill:#4A90E2
    style CW fill:#FF9500
    style GRAFANA fill:#FF6B6B
    style PAGERDUTY fill:#06A59B
    style SLACK fill:#4A154B
```

---

## 10. Cost Optimization Strategy

```mermaid
graph TD
    START([Pipeline Requirements]) --> ANALYZE[Analyze Workload<br/>Characteristics]
    
    ANALYZE --> DECISIONS{Optimization<br/>Decisions}
    
    DECISIONS --> SPOT[Use Spot Instances<br/>70% cost savings]
    DECISIONS --> AUTO[Enable Auto-Scaling<br/>2-20 instances]
    DECISIONS --> RIGHT[Right-Size Instances<br/>m5.xlarge optimal]
    DECISIONS --> CACHE[Implement Caching<br/>Reduce recomputation]
    
    SPOT --> CONFIG1[Configure Spot<br/>Bid Strategy]
    AUTO --> CONFIG2[Set Auto-Scale<br/>Policies]
    RIGHT --> CONFIG3[Select Instance<br/>Types]
    CACHE --> CONFIG4[Cache Intermediate<br/>Results]
    
    CONFIG1 --> DEPLOY[Deploy Optimized<br/>Cluster]
    CONFIG2 --> DEPLOY
    CONFIG3 --> DEPLOY
    CONFIG4 --> DEPLOY
    
    DEPLOY --> MONITOR[Monitor Costs<br/>CloudWatch]
    
    MONITOR --> RESULTS{Target<br/>Met?}
    
    RESULTS -->|Yes| SUCCESS[45% Cost Reduction<br/>Achieved]
    RESULTS -->|No| TUNE[Tune Configuration<br/>Iterate]
    
    TUNE --> ANALYZE
    
    style START fill:#90EE90
    style SUCCESS fill:#90EE90
    style SPOT fill:#FFD700
    style AUTO fill:#FFD700
```

---

## 11. Data Transformation Pipeline - Detailed Flow

```mermaid
graph TD
    RAW[Raw Data<br/>DataFrame] --> MAP[Column Mapping<br/>Standardize Names]
    
    MAP --> CLEAN[Data Cleaning<br/>- Trim whitespace<br/>- Remove duplicates<br/>- Handle nulls]
    
    CLEAN --> CAST[Type Conversions<br/>- String to Date<br/>- String to Decimal<br/>- String to Int]
    
    CAST --> STANDARDIZE[Value Standardization<br/>- Fuel types<br/>- Status codes<br/>- Currency codes]
    
    STANDARDIZE --> CALCULATE[Calculated Fields<br/>- total_amount<br/>- total_amount_usd<br/>- year/month/day]
    
    CALCULATE --> VALIDATE[Data Quality<br/>Validation]
    
    VALIDATE --> ENRICH[Data Enrichment<br/>- Add metadata<br/>- Add timestamps<br/>- Add batch IDs]
    
    ENRICH --> OUTPUT[Transformed Data<br/>Ready for Load]
    
    style RAW fill:#FFE5E5
    style OUTPUT fill:#E5FFE5
    style VALIDATE fill:#FFE5CC
```

---

## 12. Repository Structure Visualization

```
Data-engineering/
│
├── projects/
│   └── 02-etl-pipeline-framework/
│       │
│       ├── 📄 README.md                    # Project overview
│       ├── 📄 DAILY_RESPONSIBILITIES.md    # Job responsibilities
│       ├── 📄 VISUALIZATIONS.md            # This file
│       │
│       ├── 📁 config/
│       │   ├── pipeline_config.yaml              # Generic config
│       │   ├── emr_config.yaml                   # EMR settings
│       │   └── oil_gas_ticket_transactions.yaml  # Oil & Gas example
│       │
│       ├── 📁 src/
│       │   ├── framework/
│       │   │   ├── base_pipeline.py      # Core ETL framework
│       │   │   ├── spark_utils.py        # Spark management
│       │   │   ├── cdc_handler.py       # Incremental loading
│       │   │   └── cost_optimizer.py     # Cost optimization
│       │   │
│       │   ├── extractors/
│       │   │   ├── s3_extractor.py       # S3 data extraction
│       │   │   └── rds_extractor.py      # Database extraction
│       │   │
│       │   ├── transformers/
│       │   │   └── data_transformer.py   # Data transformation
│       │   │
│       │   └── loaders/
│       │       └── parquet_loader.py     # Parquet output
│       │
│       ├── 📁 pipelines/
│       │   ├── example_pipeline.py              # Basic example
│       │   ├── incremental_pipeline.py         # CDC example
│       │   └── oil_gas_ticket_pipeline.py       # Oil & Gas example
│       │
│       ├── 📁 scripts/
│       │   ├── run_pipeline.py          # Pipeline runner
│       │   └── deploy_emr_cluster.sh     # EMR deployment
│       │
│       └── 📁 tests/
│           └── test_pipeline.py          # Unit tests
│
└── README.md                             # Portfolio overview
```

---

## Key Metrics & Performance Indicators

### Pipeline Performance
- **Throughput**: 10-15M records/day
- **Data Volume**: 2-5TB daily processing
- **Processing Time**: <2 hours for full load
- **Incremental Load**: <30 minutes
- **Uptime**: >99.9%

### Cost Optimization
- **Cost Reduction**: 45% through optimization
- **Spot Instance Usage**: 70% of task nodes
- **Auto-Scaling**: 2-20 instances based on load
- **Monthly Savings**: $50K+ on AWS infrastructure

### Data Quality
- **Pass Rate**: >99.9%
- **Validation Rules**: 50+ rules
- **Error Detection**: <0.1% failure rate
- **Alert Response**: <15 minutes

---

## Technology Stack Visualization

```
┌─────────────────────────────────────────────────────────┐
│                    ETL Pipeline Framework               │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Python     │  │  Apache      │  │     AWS       │ │
│  │  PySpark     │  │   Spark      │  │     EMR        │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Delta     │  │   Parquet    │  │   S3 Data    │ │
│  │   Lake      │  │   Format     │  │   Lake       │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Great      │  │  CloudWatch  │  │   Airflow    │ │
│  │ Expectations│  │  Monitoring  │  │ Orchestration│ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

---

*These visualizations help demonstrate the architecture, data flows, and operational aspects of the ETL pipeline framework. They can be used in presentations, documentation, and portfolio demonstrations.*

