import boto3

glue = boto3.client('glue')

WORKFLOW_NAME = "RetailETL-Workflow"

def lambda_handler(event, context):
    for record in event.get('Records', []):
        key = record['s3']['object']['key']
        size = record['s3']['object'].get('size', 0)

        if not key.startswith('input/'):
            print(f"Skipping {key}: not under input/")
            continue

        if not key.lower().endswith('.csv'):
            print(f"Skipping {key}: not a CSV")
            continue

        if size == 0:
            print(f"Skipping {key}: zero-byte object")
            continue

        print(f"Valid trigger file: {key}, size={size}")
        response = glue.start_workflow_run(Name=WORKFLOW_NAME)
        print(f"Started workflow run: {response['RunId']}")

    return {"statusCode": 200}