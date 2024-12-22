import os 
import ast
import boto3
import duckdb
from langchain_openai import ChatOpenAI 

class S3DuckDBAnalyzer:
    def __init__(self, region, aws_access_key, aws_secret_key, bucket_name, duckdb_name, openai_key, model = 'gpt-4'):
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.region = region
        self.bucket_name = bucket_name
        self.conn = duckdb.connect(duckdb_name)
        self.s3_client = boto3.client(
            "s3",
            region_name=self.region,
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
        )
        self.keys_and_ext = []
        self.llm = ChatOpenAI(model = model, api_key = openai_key)

    def setup_duckdb(self):
        self.conn.execute("""
            INSTALL httpfs;
            LOAD httpfs;
        """)
        self.conn.execute(f"""
            SET s3_region = '{self.region}';
            SET s3_access_key_id = '{self.aws_access_key}';
            SET s3_secret_access_key = '{self.aws_secret_key}';
        """)

    def list_s3_files(self):
        paginator = self.s3_client.get_paginator("list_objects_v2")
        all_files = []
        for page in paginator.paginate(Bucket=self.bucket_name):
            if "Contents" in page:
                for obj in page["Contents"]:
                    all_files.append(obj['Key'])

        unique_keys = list(set(["/".join(x.split('/')[:-1]) + '/' for x in all_files]))
        self.keys_and_ext = []
        for key in unique_keys:
            all_files_related = [a for a in all_files if key in a]
            extension = all_files_related[0].split('.')[-1]
            if extension in ['json', 'parquet', 'csv']:
                self.keys_and_ext.append({'key': key, 'extension': extension})

    def create_duckdb_tables(self):
        for ke in self.keys_and_ext:
            key, ex = ke['key'], ke['extension']
            key_name = key[:-1]
            read_prefix = {
                'parquet': "read_parquet",
                'json': "read_json_auto",
                'csv': "read_csv"
            }.get(ex)

            if not read_prefix:
                print(f"Unsupported extension: {ex}")
                continue

            query = f"""
                CREATE TABLE {key_name} AS 
                SELECT * FROM {read_prefix}('s3://{self.bucket_name}/{key}*.{ex}');
            """
            self.conn.execute(query)
            
    def setup(self):
        self.setup_duckdb()
        self.list_s3_files()
        self.create_duckdb_tables()

    def get_table_schemas(self):
        schemas = []
        for ke in self.keys_and_ext:
            table = ke['key'][:-1]
            schema = self.conn.execute(f"DESCRIBE {table}").fetchall()
            schemas.append({
                'table': table,
                'schema': [(s[0], s[1]) for s in schema]
            })
        return schemas

    def generate_analysis_prompt(self, context, schemas):
        schemas_for_query = "\n".join(
            f"{s['table']}\n{str(s['schema'])}" for s in schemas
        )
        prompt = f"""
        I have data of multiple tables and files.

        {context}

        Below is the schema of all data:

        {schemas_for_query}

        I want to take insights from all these data.
        I want you to give me the business questions this data can answer.
        I also want to make a dashboard based on these business questions. 
        So please make me the business questions and DuckDB query to answer them.
        Give me in the form of list of dictionary inside of triple backticks (```).

        Here's an example:

        [{{'question': 'What are the total sales for each product?', 'query': "SELECT product_name, SUM(sales_amount) AS total_sales FROM sales_data GROUP BY product_name ORDER BY total_sales DESC;"}}]
        """
        return prompt
    
    def generate_analysis_queries(self, context):
        schemas = self.get_table_schemas()
        prompt = self.generate_analysis_prompt(context = context, schemas = schemas)
        messages = [('system', 'you are a smart assistant for a data consultant.'),
            ('human', prompt)]

        answer = self.llm.invoke(messages)
        return answer 
        
    def produce_sql_queries(self, context, schemas):
        answer = self.generate_analysis_queries(self, context, schemas)
        
        core = ast.literal_eval(answer.content.split('```')[1])
        
        os.makedirs("queries", exist_ok = True)
        
        for c in core:
            with open(f"queries/{core['question'].replace(' ', '_').lower()}.sql", 'w') as file:
                file.write(c['query'].replace("\n").strip())
            
        print("Queries succesfully created")