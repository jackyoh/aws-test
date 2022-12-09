import boto3
from moto import mock_dynamodb


@mock_dynamodb
def test_put_item():
    table_name = "table1"
    dyn_client = boto3.client("dynamodb", region_name="ap-southeast-1")
    dyn_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "userId", "AttributeType": "S"},
            {"AttributeName": "title", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "userId", "KeyType": "HASH"},
            {"AttributeName": "title", "KeyType": "RANGE"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    dyn_client.batch_write_item(RequestItems={
        table_name: [
            {
                "PutRequest": {
                    "Item": {
                        "userId": {"S": "user001"},
                        "title": {"S": "title1"},
                    }
                },
            },
            {
                "PutRequest": {
                    "Item": {
                        "userId": {"S": "user001"},
                        "title": {"S": "title2"},
                    }
                },
            },
            {
                "PutRequest": {
                    "Item": {
                        "userId": {"S": "user002"},
                        "title": {"S": "title3"},
                    }
                }
            },
        ]
    })
    dyn_resource = boto3.resource("dynamodb", region_name="ap-southeast-1")
    table = dyn_resource.Table("table1")
    items = table.scan()
    assert items["Count"] == 3


@mock_dynamodb
def test_delete_item():
    table_name = "table1"
    dyn_client = boto3.client("dynamodb", region_name="ap-southeast-1")
    dyn_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "userId", "AttributeType": "S"},
            {"AttributeName": "title", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "userId", "KeyType": "HASH"},
            {"AttributeName": "title", "KeyType": "RANGE"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    dyn_resource = boto3.resource("dynamodb", region_name="ap-southeast-1")
    table = dyn_resource.Table("table1")
    table.put_item(Item={
        "userId": "user001",
        "title": "title1",
    })
    dyn_client.batch_write_item(RequestItems={
        table_name: [
            {
                "DeleteRequest": {
                    "Key": {
                        "userId": {"S": "user001"},
                        "title": {"S": "title1"},
                    }
                },
            },
        ]
    })
    items = table.scan()
    assert items["Count"] == 0
