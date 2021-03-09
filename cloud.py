import boto3
s3 = boto3.resource('s3',
                    aws_access_key_id='AKIAIPSDFXIBBC3RD5AA',
                    aws_secret_access_key='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')
try:
    s3.create_bucket(Bucket='datacont', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
except:
    print("This may already exist")

s3.Object('datacont', 'test.jpg').put(
    Body=open('test.jpg', 'rb')
)
