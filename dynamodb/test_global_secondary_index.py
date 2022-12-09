import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb


@mock_dynamodb
def test_global_secondary_index():
    table_name = "table1"
    dyn_resource = boto3.resource("dynamodb", region_name="ap-southeast-1")
    table = dyn_resource.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "userId", "AttributeType": "S"},
            {"AttributeName": "createdAt", "AttributeType": "N"},
            {"AttributeName": "messageType", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "userId", "KeyType": "HASH"},
            {"AttributeName": "createdAt", "KeyType": "RANGE"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "tableMessageTypeIndex",
                "KeySchema": [
                    {"AttributeName": "messageType", "KeyType": "HASH"},
                    {"AttributeName": "createdAt", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.put_item(Item={
        "userId": "user001",
        "messageType": "inbox",
        "createdAt": 3,
        "content": "inbox_3",
    })
    table.put_item(Item={
        "userId": "user001",
        "messageType": "marquee",
        "createdAt": 1,
        "content": "marquee_1",
    })
    table.put_item(Item={
        "userId": "user002",
        "messageType": "inbox",
        "createdAt": 1,
        "content": "inbox_1",
    })
    table.put_item(Item={
        "userId": "user003",
        "messageType": "inbox",
        "createdAt": 5,
        "content": "inbox_5",
    })
    table.put_item(Item={
        "userId": "user003",
        "messageType": "inbox",
        "createdAt": 4,
        "content": "inbox_4",
    })
    result = table.query(
        KeyConditionExpression=Key("userId").eq("user001"),
    )
    items = result["Items"]
    assert items[0]["content"] == "marquee_1"
    assert items[1]["content"] == "inbox_3"

    result = table.query(
        KeyConditionExpression=Key("messageType").eq("inbox"),
        IndexName="tableMessageTypeIndex"
    )
    items = result["Items"]
    assert items[0]["content"] == "inbox_1"
    assert items[1]["content"] == "inbox_3"
    assert items[2]["content"] == "inbox_4"
    assert items[3]["content"] == "inbox_5"
