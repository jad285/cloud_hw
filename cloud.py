import boto3
import csv
s3 = boto3.resource('s3',
                    aws_access_key_id='OMITTED',
                    aws_secret_access_key='OMITTED')

# Step 1: Create the Bucket
try:
    s3.create_bucket(Bucket='datacont-hwbucket', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
    print("Created")
except:
    print("This may already exist")

# Step 2: Make the Bucket Publicly Readable
bucket = s3.Bucket('datacont-hwbucket')
bucket.Acl().put(ACL='public-read')

# Step 3: Upload a file into the bucket
body = open('./CSV/exp1.csv', 'rb')
o = s3.Object('datacont-hwbucket', 'test').put(Body=body)
s3.Object('datacont-hwbucket', 'test').Acl().put(ACL='public-read')

# Step 4: Create the DynamoDB Table
dyndb = boto3.resource('dynamodb',
                       region_name='us-west-2',
                       aws_access_key_id='OMITTED',
                       aws_secret_access_key='OMITTED'
                       )

try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'PartitionKey',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PartitionKey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    table = dyndb.Table("DataTable")
    print("Table already exists")

table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
print(table.item_count)

# Step 5: Reading the csv file, uploading the blobs, and creating the table
table = dyndb.Table("DataTable")
with open('./CSV/experiments.csv', 'r', encoding="utf-8") as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        body = open('./CSV/'+item[4], 'rb')
        s3.Object('datacont-hwbucket', item[4]).put(Body=body)
        md = s3.Object('datacont-hwbucket', item[4]).Acl().put(ACL='public-read')

        url = "https://s3-us-west-2.amazonaws.com/datacont-hwbucket/"+item[4]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
                         'description': item[3], 'date': item[2], 'url': url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

# Step 6: Search For an Item (Query)
response = table.get_item(
    Key={
        'PartitionKey': 'experiment1',
        'RowKey': 'data1'
    }
)
item = response['Item']
print(item)
