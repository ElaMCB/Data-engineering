# ETL Pipeline Framework - 3-Day Learning Tutorial
## Learn Data Engineering in Simple Terms

> **Goal**: Understand how to build production-ready ETL pipelines in 3 days  
> **Prerequisites**: Basic Python knowledge (helpful but not required)  
> **Approach**: Learn by understanding concepts, then see them in practice

---

## 📅 Day 1: Understanding the Basics

### What is ETL? (Extract, Transform, Load)

Think of ETL like a **factory assembly line**:

1. **Extract** = Get raw materials (data) from suppliers (databases, files)
2. **Transform** = Clean, shape, and improve the materials (clean and standardize data)
3. **Load** = Put finished products in the warehouse (save processed data)

**Real-world example**: 
- You have messy customer data from 5 different systems
- ETL cleans it up, makes it consistent, and puts it in one place
- Now you can analyze it easily!

---

### Lesson 1.1: What is Data Engineering?

**In Simple Terms:**
Data engineering is like being a **plumber for data**. You:
- Build pipes (pipelines) that move data from one place to another
- Make sure data flows smoothly and doesn't leak (errors)
- Ensure data is clean and usable when it arrives

**Why It Matters:**
- Companies have data everywhere (databases, files, APIs)
- Data needs to be cleaned and organized to be useful
- Data engineers build the systems that make this happen automatically

---

### Lesson 1.2: Understanding the ETL Pipeline Framework

**Think of it like a recipe:**

```
Recipe for Data Processing:
1. Get ingredients (Extract data)
2. Prepare and cook (Transform data)
3. Serve the dish (Load data)
```

**Our Framework Components:**

#### 1. **Extractor** (Getting Data)
- **S3 Extractor**: Gets data from cloud storage (like Amazon's storage)
- **RDS Extractor**: Gets data from databases (like PostgreSQL, MySQL)

**Simple Example:**
```python
# Imagine you're reading a book
extractor = S3Extractor(
    source_path="s3://my-bucket/data/"  # Where the book is
)
data = extractor.extract()  # Read the book
```

#### 2. **Transformer** (Cleaning Data)
- Removes errors and inconsistencies
- Standardizes formats (dates, names, etc.)
- Validates data quality

**Simple Example:**
```python
# Imagine cleaning a messy room
transformer = DataTransformer()
# Before: "JOHN DOE", "john doe", "John Doe"
# After:  "John Doe" (all standardized)
clean_data = transformer.transform(data)
```

#### 3. **Loader** (Saving Data)
- Saves processed data in the right format
- Organizes it efficiently (like filing cabinets)
- Makes it easy to find later

**Simple Example:**
```python
# Imagine saving files in organized folders
loader = ParquetLoader(
    target_path="s3://my-bucket/processed-data/"
)
loader.load(clean_data)  # Save the cleaned data
```

---

### Lesson 1.3: What is Apache Spark?

**In Simple Terms:**
Apache Spark is like having **many workers** process data at the same time instead of one person doing it slowly.

**Analogy:**
- **Without Spark**: One person sorting 1 million papers (slow!)
- **With Spark**: 100 people each sorting 10,000 papers (fast!)

**Why We Use It:**
- Handles HUGE amounts of data (terabytes)
- Processes data in parallel (many tasks at once)
- Works on cloud computers (AWS EMR)

**Key Concept:**
```
Traditional Processing:
Data → Computer → Result (slow for big data)

Spark Processing:
Data → Split into chunks → Many computers process → Combine results (fast!)
```

---

### Lesson 1.4: Understanding Data Formats

#### Parquet Format
**Think of it like a filing cabinet:**
- Traditional files (CSV) = papers in a box (slow to search)
- Parquet = organized filing cabinet (fast to find what you need)

**Benefits:**
- Faster to read
- Takes less storage space
- Better for big data

#### Delta Lake
**Think of it like a version-controlled document:**
- You can see history of changes
- Can undo mistakes
- Better for data that changes over time

---

### Day 1 Practice Exercise

**Try to understand:**
1. What does ETL stand for?
2. Why do we need to transform data?
3. What is the benefit of using Spark?

**Answer Key:**
1. Extract, Transform, Load - the three steps of data processing
2. Raw data is messy and inconsistent; transformation makes it usable
3. Spark processes data much faster by using multiple computers in parallel

---

## 📅 Day 2: Building Your First Pipeline

### Lesson 2.1: Understanding the Pipeline Configuration (YAML)

**What is YAML?**
YAML is like a **recipe card** that tells the pipeline what to do.

**Simple Example:**
```yaml
# This is like a recipe
extract:
  source_path: "s3://bucket/data/"  # Where to get data
  file_format: "parquet"            # What type of file

transform:
  column_mappings:                   # Rename columns
    old_name: "new_name"

load:
  target_path: "s3://bucket/output/" # Where to save
```

**Breaking It Down:**
- `extract`: Where to get the data from
- `transform`: How to clean and change the data
- `load`: Where to save the processed data

---

### Lesson 2.2: Understanding Data Transformations

**Why Transform Data?**

Real-world data is messy:
```
Before Transformation:
- Names: "JOHN", "john", "John Doe", "J. Doe"
- Dates: "01/15/2024", "2024-01-15", "Jan 15, 2024"
- Amounts: "$100.50", "100.50", "100.5"

After Transformation:
- Names: "John Doe" (all standardized)
- Dates: "2024-01-15" (all same format)
- Amounts: 100.50 (all numbers)
```

**Common Transformations:**

1. **Column Mapping** (Renaming)
   ```yaml
   column_mappings:
     user_id: "customer_id"  # Change "user_id" to "customer_id"
   ```

2. **Type Conversion** (Changing Data Types)
   ```yaml
   transformations:
     - type: "cast"
       column: "amount"
       target_type: "decimal(10,2)"  # Make it a number with 2 decimals
   ```

3. **Value Standardization** (Making Values Consistent)
   ```yaml
   - type: "replace"
     column: "status"
     old_value: "P"
     new_value: "Pending"  # Change "P" to "Pending"
   ```

4. **Default Values** (Filling Missing Data)
   ```yaml
   - type: "default_value"
     column: "status"
     default: "active"  # If status is missing, use "active"
   ```

---

### Lesson 2.3: Understanding Data Quality Rules

**Why Check Data Quality?**

Imagine a bank processing transactions:
- Missing account numbers = bad!
- Negative amounts when they should be positive = bad!
- Invalid dates = bad!

**Data Quality Rules Check:**

1. **Not Null** (Required Fields)
   ```yaml
   - type: "not_null"
     column: "customer_id"  # This field MUST have a value
   ```

2. **Value Range** (Reasonable Limits)
   ```yaml
   - type: "value_range"
     column: "age"
     min: 0
     max: 120  # Age must be between 0 and 120
   ```

3. **Allowed Values** (Valid Options Only)
   ```yaml
   - type: "allowed_values"
     column: "status"
     allowed_values: ["active", "inactive", "pending"]  # Only these values allowed
   ```

**Real Example from Oil & Gas Pipeline:**
```yaml
# Check that quantity is reasonable
- type: "value_range"
  column: "quantity"
  min: 0.01        # Minimum 0.01 barrels
  max: 5000000     # Maximum 5M barrels (very large tanker)
```

---

### Lesson 2.4: Understanding Incremental Loading (CDC)

**The Problem:**
- You have 10 million records
- Only 1,000 changed today
- Do you process all 10 million again? (wasteful!)

**The Solution: CDC (Change Data Capture)**
Only process what changed!

**Simple Analogy:**
- **Full Load**: Re-reading entire book to find one changed sentence
- **Incremental Load**: Only reading the pages that changed

**How It Works:**
```
1. Remember last time you processed data (timestamp)
2. Only get records that changed since then
3. Update only those records in the target
```

**Example:**
```python
# Last processed: January 1, 2024 at 2:00 AM
# Now: January 2, 2024 at 2:00 AM
# Only get records updated between these times
cdc_handler.run_incremental_load(lookback_hours=24)
```

**Benefits:**
- Much faster (process 1,000 records vs 10 million)
- Less computing cost
- Real-time updates possible

---

### Lesson 2.5: Understanding Cost Optimization

**The Challenge:**
- Running big data pipelines costs money (AWS charges for compute)
- Want to process data fast AND cheap

**Cost Optimization Strategies:**

#### 1. **Spot Instances** (Like Buying Plane Tickets Early)
- Regular instances: $1/hour
- Spot instances: $0.30/hour (70% cheaper!)
- Risk: Can be interrupted (but we handle this)

#### 2. **Auto-Scaling** (Like Hiring Workers Only When Needed)
- Don't keep 20 workers all day
- Start with 2 workers
- Add more when busy (up to 20)
- Remove when not busy

**Example:**
```
Normal Setup:
- Always 10 computers running = $10/hour × 24 hours = $240/day

Optimized Setup:
- 2 computers normally, scale to 10 when needed
- Average 4 computers = $4/hour × 24 hours = $96/day
- Savings: $144/day (60% cheaper!)
```

#### 3. **Right-Sizing** (Using the Right Tool for the Job)
- Don't use a truck to deliver a letter
- Use appropriate computer size for the workload

---

### Day 2 Practice Exercise

**Create a simple transformation:**
```yaml
# Your task: Standardize customer status codes
transformations:
  - type: "replace"
    column: "status"
    old_value: "A"
    new_value: "Active"
  # Add more replacements for "I" → "Inactive", "P" → "Pending"
```

**Answer:**
```yaml
transformations:
  - type: "replace"
    column: "status"
    old_value: "A"
    new_value: "Active"
  - type: "replace"
    column: "status"
    old_value: "I"
    new_value: "Inactive"
  - type: "replace"
    column: "status"
    old_value: "P"
    new_value: "Pending"
```

---

## 📅 Day 3: Putting It All Together

### Lesson 3.1: Understanding the Complete Pipeline Flow

**Let's trace through a real example:**

**Scenario**: Processing oil & gas carrier ticket transactions

```
Step 1: EXTRACT
--------------
Source: S3 bucket with transaction files
- File: transactions_2024_01_15.parquet
- Contains: 10 million ticket transactions

Step 2: TRANSFORM
-----------------
1. Rename columns:
   - "ticket_num" → "ticket_id"
   - "txn_date" → "transaction_date"

2. Standardize values:
   - "CRUDE" → "Crude Oil"
   - "P" → "Pending"

3. Calculate fields:
   - total_amount = quantity × price_per_unit
   - Extract year, month, day from date

4. Validate:
   - Check quantity is between 0.01 and 5,000,000
   - Check all required fields are present

Step 3: LOAD
------------
Save to: s3://processed-data/tickets/
- Partitioned by: year=2024/month=01/day=15/fuel_type=Crude Oil
- Format: Parquet (compressed)
- Result: Clean, organized, queryable data
```

---

### Lesson 3.2: Understanding the Oil & Gas Example

**Why This Example is Good to Learn From:**

1. **Real-World Scenario**: Oil & gas companies process millions of transactions daily
2. **Complex Data**: Multiple fuel types, carriers, vessels, routes
3. **Business Rules**: Specific validations (quantities, prices, dates)
4. **Scale**: Handles 10-15 million records per day

**Key Fields Explained:**

| Field | What It Means | Example |
|-------|---------------|---------|
| `ticket_id` | Unique transaction ID | "TKT-2024-001234" |
| `carrier_id` | Which shipping company | "CARRIER-001" |
| `vessel_id` | Which ship | "VESSEL-ABC-123" |
| `fuel_type` | Type of oil/gas | "Crude Oil", "LNG" |
| `quantity` | How much | 50000 (barrels) |
| `price_per_unit` | Cost per unit | 75.50 (dollars) |
| `total_amount` | Total cost | 3,775,000 (dollars) |
| `origin_port` | Where it starts | "HOUSTON" |
| `destination_port` | Where it goes | "ROTTERDAM" |

**Why We Transform:**
- Source system uses codes: "CRUDE", "LNG"
- We standardize: "Crude Oil", "Liquefied Natural Gas"
- Makes reporting and analysis easier

---

### Lesson 3.3: Understanding Monitoring and Alerts

**Why Monitor Pipelines?**

**Analogy**: Like a car dashboard
- Speedometer = How fast pipeline runs
- Fuel gauge = How much data processed
- Warning lights = Problems detected

**What We Monitor:**

1. **Pipeline Health**
   - Did it run successfully?
   - How long did it take?
   - How many records processed?

2. **Data Quality**
   - How many records failed validation?
   - Are there anomalies?

3. **Costs**
   - How much did this run cost?
   - Are we staying within budget?

**Example Alerts:**
```
Alert 1: Pipeline Failed
- What: Pipeline execution failed
- When: Immediately
- Action: Investigate and fix

Alert 2: High Error Rate
- What: >100 records failed validation
- When: After pipeline completes
- Action: Review data quality issues

Alert 3: Slow Performance
- What: Pipeline took >2 hours
- When: After pipeline completes
- Action: Optimize performance
```

---

### Lesson 3.4: Understanding the Code Structure

**How the Framework is Organized:**

```
src/
├── framework/          # Core building blocks
│   ├── base_pipeline.py    # Main pipeline class
│   ├── spark_utils.py      # Spark helper functions
│   ├── cdc_handler.py       # Incremental loading
│   └── cost_optimizer.py    # Cost optimization
│
├── extractors/         # How to get data
│   ├── s3_extractor.py      # From S3 storage
│   └── rds_extractor.py     # From databases
│
├── transformers/       # How to clean data
│   └── data_transformer.py  # All transformations
│
└── loaders/           # How to save data
    └── parquet_loader.py    # Save as Parquet
```

**Simple Explanation:**

1. **Framework**: The foundation (like the foundation of a house)
2. **Extractors**: Tools to get data (like different types of shovels)
3. **Transformers**: Tools to clean data (like cleaning supplies)
4. **Loaders**: Tools to save data (like storage boxes)

**How They Work Together:**

```python
# Step 1: Get data
extractor = S3Extractor(source_path="s3://data/")
raw_data = extractor.extract()

# Step 2: Clean data
transformer = DataTransformer()
clean_data = transformer.transform(raw_data)

# Step 3: Save data
loader = ParquetLoader(target_path="s3://output/")
loader.load(clean_data)
```

---

### Lesson 3.5: Common Patterns and Best Practices

#### Pattern 1: Always Validate Data
```yaml
# Good practice: Check data before using it
data_quality_rules:
  - type: "not_null"
    column: "customer_id"  # Must have customer ID
```

#### Pattern 2: Use Incremental Loading
```python
# Good practice: Only process what changed
cdc_handler.run_incremental_load(lookback_hours=24)
# Instead of processing everything every time
```

#### Pattern 3: Partition Data
```yaml
# Good practice: Organize data for fast queries
partition_by:
  - "year"
  - "month"
  - "day"
# Like organizing files by date in folders
```

#### Pattern 4: Monitor Everything
```python
# Good practice: Track what's happening
results = pipeline.run()
# Log metrics, errors, performance
```

---

### Day 3 Practice Exercise

**Build a Simple Pipeline Configuration:**

**Scenario**: Process customer orders

**Requirements:**
1. Extract from S3: `s3://orders/raw/`
2. Transform:
   - Rename "order_id" to "id"
   - Standardize status: "C" → "Completed", "P" → "Pending"
   - Validate: amount > 0
3. Load to: `s3://orders/processed/`
4. Partition by: year, month

**Try to write the YAML configuration:**

<details>
<summary>Click to see answer</summary>

```yaml
extract:
  source_path: "s3://orders/raw/"
  file_format: "parquet"

transform:
  column_mappings:
    order_id: "id"
  
  transformations:
    - type: "replace"
      column: "status"
      old_value: "C"
      new_value: "Completed"
    - type: "replace"
      column: "status"
      old_value: "P"
      new_value: "Pending"
  
  data_quality_rules:
    - type: "value_range"
      column: "amount"
      min: 0.01

load:
  target_path: "s3://orders/processed/"
  partition_by:
    - "year"
    - "month"
```
</details>

---

## 🎓 Quick Reference Guide

### Key Terms Glossary

| Term | Simple Explanation |
|------|-------------------|
| **ETL** | Extract (get), Transform (clean), Load (save) |
| **Pipeline** | Automated process that moves and processes data |
| **Spark** | Tool that processes big data fast using many computers |
| **Parquet** | Efficient file format for storing data |
| **CDC** | Only process data that changed (incremental loading) |
| **Partition** | Organizing data into folders (like organizing files) |
| **YAML** | Configuration file format (like a recipe card) |
| **Data Quality** | Making sure data is correct and complete |
| **Spot Instance** | Cheaper cloud computers (can be interrupted) |
| **Auto-Scaling** | Automatically adding/removing computers as needed |

---

### Common Patterns Cheat Sheet

#### 1. Standard Pipeline Setup
```python
# Get data
extractor = S3Extractor(source_path="s3://input/")
data = extractor.extract()

# Clean data
transformer = DataTransformer()
clean_data = transformer.transform(data)

# Save data
loader = ParquetLoader(target_path="s3://output/")
loader.load(clean_data)
```

#### 2. Incremental Loading
```python
cdc_handler = CDCHandler(
    key_columns=["id"],
    timestamp_column="updated_at"
)
cdc_handler.run_incremental_load(lookback_hours=24)
```

#### 3. Data Quality Check
```yaml
data_quality_rules:
  - type: "not_null"
    column: "required_field"
  - type: "value_range"
    column: "amount"
    min: 0
    max: 1000000
```

---

## 🚀 Next Steps

### After Completing This Tutorial:

1. **Review the Code**
   - Look at `pipelines/example_pipeline.py`
   - See how everything connects

2. **Study the Oil & Gas Example**
   - Read `config/oil_gas_ticket_transactions.yaml`
   - Understand real-world transformations

3. **Explore the Visualizations**
   - Check `VISUALIZATIONS.md`
   - See how everything fits together visually

4. **Practice**
   - Try modifying the configuration
   - Create your own transformation rules
   - Experiment with different data quality rules

### Resources for Further Learning:

1. **Apache Spark Documentation**
   - Learn more about Spark operations
   - Understand DataFrame operations

2. **AWS EMR Documentation**
   - Learn about cloud computing
   - Understand cluster management

3. **Python/PySpark Tutorials**
   - Practice Python basics
   - Learn PySpark operations

---

## ❓ Frequently Asked Questions

### Q: Do I need to know Python to use this?
**A**: Basic Python helps, but you can start with just understanding the YAML configurations. The framework handles the Python code for you.

### Q: What if I don't have AWS?
**A**: You can run Spark locally for learning. The concepts are the same, just on a smaller scale.

### Q: How do I test this?
**A**: Start with small sample data. Use the test files in the `tests/` directory as examples.

### Q: What's the hardest part?
**A**: Understanding how all the pieces fit together. Start simple, then add complexity.

### Q: Can I use this for my own projects?
**A**: Yes! This is a production-ready framework. Customize it for your needs.

---

## 📝 Summary

**What You Learned in 3 Days:**

✅ **Day 1**: What ETL is, why we need it, and the basic components  
✅ **Day 2**: How to configure pipelines, transform data, and optimize costs  
✅ **Day 3**: How everything works together in a real-world example  

**Key Takeaways:**

1. **ETL = Extract, Transform, Load** - The three steps of data processing
2. **Spark = Fast processing** - Uses many computers in parallel
3. **Configuration = Recipe** - YAML files tell the pipeline what to do
4. **Data Quality = Important** - Always validate your data
5. **Cost Matters** - Optimize to save money (spot instances, auto-scaling)

**You're Now Ready To:**
- Understand how ETL pipelines work
- Read and modify pipeline configurations
- Understand the code structure
- Apply these concepts to your own projects

---

*Happy Learning! 🎉*

*Remember: Start simple, practice often, and don't be afraid to experiment!*

