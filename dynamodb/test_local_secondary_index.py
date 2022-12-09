import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb


@mock_dynamodb
def test_local_secondary_index():
    table_name = "table1"
    dyn_resource = boto3.resource("dynamodb", region_name="ap-southeast-1")
    table = dyn_resource.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "userId", "AttributeType": "S"},
            {"AttributeName": "title", "AttributeType": "S"},
            {"AttributeName": "createdAt", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "userId", "KeyType": "HASH"},
            {"AttributeName": "title", "KeyType": "RANGE"}
        ],
        LocalSecondaryIndexes=[
            {
                "IndexName": "tableCreatedAtIndex",
                "KeySchema": [
                    {"AttributeName": "userId", "KeyType": "HASH"},
                    {"AttributeName": "createdAt", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.put_item(
        Item={
            "userId": "user001",
            "title": "aaa",
            "createdAt": 1,
        }
    )
    table.put_item(
        Item={
            "userId": "user001",
            "title": "ddd",
            "createdAt": 3,
        }
    )
    table.put_item(
        Item={
            "userId": "user001",
            "title": "ccc",
            "createdAt": 2,
        }
    )
    result = table.scan()
    items = result["Items"]
    assert items[0]["title"] == "aaa"
    assert items[1]["title"] == "ddd"
    assert items[2]["title"] == "ccc"

    result = table.query(
        KeyConditionExpression=Key("userId").eq("user001"),
        IndexName="tableCreatedAtIndex"
    )
    items = result["Items"]
    assert items[0]["title"] == "aaa"
    assert items[1]["title"] == "ccc"
    assert items[2]["title"] == "ddd"
