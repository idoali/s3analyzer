from src.s3_duckdb_analyzer import S3DuckDBAnalyzer

test = S3DuckDBAnalyzer(
    region = '***',
    aws_access_key = '***',
    aws_secret_key = '***',
    bucket_name = '***',
    duckdb_name = '***',
    openai_key = '***',
)

test.setup()
context = "The data contains transaction records from an online shoes store."
queries = test.generate_analysis_queries(context)
