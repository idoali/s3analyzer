# S3 Analyzer

This is a framework to analyze your S3 bucket using DuckDB. A lot of times, people have a lot of data but have no idea what to look for on the data. This framework could help you to find insights out of the data. 

### Requirements

- Python 
- S3 Bucket filled with data
- OpenAI API
- Libraries:

```
duckdb==1.1.3
boto3==1.35.65
langchain-openai==0.2.12
```

### How to Use

**Initialize the class:**
```python
test = S3DuckDBAnalyzer(
    region='your-region',
    aws_access_key='your-access-key',
    aws_secret_key='your-secret-key',
    bucket_name='your-bucket-name',
    duckdb_name='your-duckdb-name',
    openai_key='your-openai-key',
)
```
This sets up your S3 client, DuckDB connection, and GPT-4 access.

**Run the setup:**
```python
test.setup()
```
This loads the S3 files, creates DuckDB tables, and gets everything ready.

**Provide context and get queries:**
```python
context = """These files contain metadata for all 45,000 movies listed in the Full MovieLens Dataset..."""
queries = test.generate_analysis_queries(context)
```
The `context` is just a short description of your data. The function sends this to GPT-4 along with table schemas and gets back business questions and SQL queries.

**Save queries to disk (optional):**
```python
test.produce_sql_queries(context)
```

This creates `.sql` files for each query in a folder called `queries`.
