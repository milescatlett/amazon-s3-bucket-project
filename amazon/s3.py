import boto3
import pandas
from bs4 import BeautifulSoup
import cr

access_key = cr.access_key
secret_key = cr.secret_key

# Creating the low level functional client
client = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name='ap-south-1'
)

# Creating the high level object oriented interface
resource = boto3.resource(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name='ap-south-1'
)

# Fetch the list of existing buckets
clientResponse = client.list_buckets()

# Print the bucket names one by one
bucket = ''
for name in clientResponse['Buckets']:
    bucket = name
bucket = bucket['Name']
response = client.list_objects(Bucket=bucket).get("Contents")
key = ''
for i in range(len(response)):
    key = response[i]['Key']
# Create the S3 object
obj = client.get_object(
    Bucket=bucket,
    Key=key
)
pd_data = pandas.read_csv('qai_headers.csv')
headers = pd_data['Header'].tolist()
file = obj['Body']
contents = ''
for item in file:
    contents += item.decode()
soup = BeautifulSoup(contents, 'xml')
answers = soup.find_all('answer')
count = 0
for answer in answers:
    count += 1
    label = answer.get('label')
    question = answer.find('question').text
    val = answer.find('value')
    if val is not None and question != "Photos" and "Signature" not in question:
        val = answer.find('value').text
    else:
        val = ''
    print(f"LABEL: {label}, QUES: {question}, VALUE: {val}")
print(count)
