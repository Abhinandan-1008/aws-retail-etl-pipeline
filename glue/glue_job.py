import sys
import boto3
import pandas as pd
from io import StringIO
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'input_bucket', 'input_key', 'output_bucket', 'output_key']
)

INPUT_BUCKET = args['input_bucket']
INPUT_KEY = args['input_key']
OUTPUT_BUCKET = args['output_bucket']
OUTPUT_KEY = args['output_key']

s3 = boto3.client('s3')

def main():
    obj = s3.get_object(Bucket=INPUT_BUCKET, Key=INPUT_KEY)
    df = pd.read_csv(obj['Body'])

    print("Columns found:", list(df.columns))

    df = df.dropna(how='all')
    df['customer_name'] = df['customer_name'].astype(str).str.upper()
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['discounted_amount'] = (df['price'] * 0.90).round(2)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=OUTPUT_KEY,
        Body=csv_buffer.getvalue()
    )

    print(f"Transformed {len(df)} rows.")
    print(f"Wrote output to s3://{OUTPUT_BUCKET}/{OUTPUT_KEY}")

if __name__ == '__main__':
    main()