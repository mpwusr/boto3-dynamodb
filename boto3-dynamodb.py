import boto3
from boto3.dynamodb.conditions import Key, Attr

# Get the service resource.
__db_resource = ''


def __init__():
    __db_resource = get_dynamodb_resource('dynamodb')


def get_dynamodb_resource(db_name):
    dynamodb = boto3.resource('dynamodb')
    return dynamodb


# Create the DynamoDB table.
def create_dynamo_table(dbresource, table_name):
    table = dbresource.create_table(
        TableName='users',
        KeySchema=[
            {
                'AttributeName': 'username',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'last_name',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'username',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'last_name',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName='users')

    # Print out some data about the table.
    print(table.item_count)

    # Instantiate a table resource object without actually
    # creating a DynamoDB table. Note that the attributes of this table
    # are lazy-loaded: a request is not made nor are the attribute
    # values populated until the attributes
    # on the table resource are accessed or its load() method is called.
    table = dbresource.Table('users')

    # Print out some data about the table.
    # This will cause a request to be made to DynamoDB and its attribute
    # values will be set based on the response.
    print(table.creation_date_time)


def table_put_item(table, item_data):
    table.put_item(
        Item={
            'username': 'janedoe',
            'first_name': 'Jane',
            'last_name': 'Doe',
            'age': 25,
            'account_type': 'standard_user',
        }
    )


def table_get_item(table, item_key):
    response = table.get_item(
        Key={
            'username': 'janedoe',
            'last_name': 'Doe'
        }
    )
    item = response['Item']
    print(item)


def table_update_item(table, item_key):
    table.update_item(
        Key={
            'username': 'janedoe',
            'last_name': 'Doe'
        },
        UpdateExpression='SET age = :val1',
        ExpressionAttributeValues={
            ':val1': 26
        }
    )


def table_delete_item(table, item_key):
    table.delete_item(
        Key={
            'username': 'janedoe',
            'last_name': 'Doe'
        }
    )


def table_batch_write(table, batch_items):
    with table.batch_writer() as batch:
        batch.put_item(
            Item={
                'account_type': 'standard_user',
                'username': 'johndoe',
                'first_name': 'John',
                'last_name': 'Doe',
                'age': 25,
                'address':
                    {
                        'road': '1 Jefferson Street',
                        'city': 'Los Angeles',
                        'state': 'CA',
                        'zipcode': 90001
                    }
            }
        )


def table_batch_put_items(batch):
    batch.put_item(
        Item={
            'account_type': 'super_user',
            'username': 'janedoering',
            'first_name': 'Jane',
            'last_name': 'Doering',
            'age': 40,
            'address': {
                'road': '2 Washington Avenue',
                'city': 'Seattle',
                'state': 'WA',
                'zipcode': 98109
            }
        }
    )

    batch.put_item(
        Item={
            'account_type': 'standard_user',
            'username': 'bobsmith',
            'first_name': 'Bob',
            'last_name': 'Smith',
            'age': 18,
            'address': {
                'road': '3 Madison Lane',
                'city': 'Louisville',
                'state': 'KY',
                'zipcode': 40213
            }
        }
    )
    batch.put_item(
        Item={
            'account_type': 'super_user',
            'username': 'alicedoe',
            'first_name': 'Alice',
            'last_name': 'Doe',
            'age': 27,
            'address': {
                'road': '1 Jefferson Street',
                'city': 'Los Angeles',
                'state': 'CA',
                'zipcode': 90001
            }
        }
    )


def bulk_batch_writer(table_name):
    with table_name.batch_writer() as batch:
        for i in range(50):
            batch.put_item(
                Item={
                    'account_type': 'anonymous',
                    'username': 'user' + str(i),
                    'first_name': 'unknown',
                    'last_name': 'unknown'
                }
            )


def bulk_batch_writer_keys(table_name, part_key, sort_key):
    with table_name.batch_writer(overwrite_by_pkeys=['partition_key', 'sort_key']) as batch:
        batch.put_item(
            Item={
                'partition_key': 'p1',
                'sort_key': 's1',
                'other': '111',
            }
        )
        batch.put_item(
            Item={
                'partition_key': 'p1',
                'sort_key': 's1',
                'other': '222',
            }
        )


def delete_item_from_table(batch, part_key, sort_key):
    batch.delete_item(
        Key={
            'partition_key': 'p1',
            'sort_key': 's2'
        }
    )

def table_query(table_name, key, match):

    response = table_name.query(
        KeyConditionExpression=Key('username').eq('johndoe')
    )
    items = response['Items']
    print(items)


def table_scan(table_name, key, match):
    response = table_name.scan(
        FilterExpression=Attr('address.state').eq('CA')
    )
    items = response['Items']
    print(items)
