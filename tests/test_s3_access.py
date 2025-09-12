import boto3
from dotenv import load_dotenv
import os

load_dotenv()

try:
    # Create S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )
    
    # Test basic S3 access
    response = s3_client.list_buckets()
    print("✅ AWS credentials are valid")
    print(f"Available buckets: {[bucket['Name'] for bucket in response['Buckets']]}")
    
    # Test specific bucket access (replace with your bucket name)
    bucket_name = os.getenv('S3_BUCKET_NAME')  # Update this
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        print(f"✅ Can access bucket: {bucket_name}")
    except Exception as e:
        print(f"❌ Cannot access bucket {bucket_name}: {e}")
        
except Exception as e:
    print(f"❌ AWS credentials error: {e}")