import boto3
import datetime

from moto import mock_dynamodb


@mock_dynamodb
def test_put_item():
    table_name = "table1"
    dyn_resource = boto3.resource("dynamodb", region_name="ap-southeast-1")
    table = dyn_resource.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "userId", "AttributeType": "S"},
            {"AttributeName": "createdAt", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "userId", "KeyType": "HASH"},
            {"AttributeName": "createdAt", "KeyType": "RANGE"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    table.put_item(
        Item={
            "userId": "user001",
            "createdAt": int(datetime.datetime.now().timestamp() * 1000),
            "address": "address1",
        }
    )
    result = table.scan()
    assert result["Count"] == 1


@mock_dynamodb
def test_delete_item():
    table_name = "table1"
    dyn_resource = boto3.resource("dynamodb", region_name="ap-southeast-1")
    table = dyn_resource.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "userId", "AttributeType": "S"},
            {"AttributeName": "createdAt", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "userId", "KeyType": "HASH"},
            {"AttributeName": "createdAt", "KeyType": "RANGE"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    now_date = int(datetime.datetime.now().timestamp() * 1000)
    table.put_item(
        Item={
            "userId": "user001",
            "createdAt": now_date,
            "address": "address1",
        }
    )
    result = table.scan()
    assert result["Count"] == 1
    table.delete_item(
        Key={
            "userId": "user001",
            "createdAt": now_date,
        }
    )
    result = table.scan()
    assert result["Count"] == 0


@mock_dynamodb
def test_update_item():
    table_name = "table1"
    dyn_resource = boto3.resource("dynamodb", region_name="ap-southeast-1")
    table = dyn_resource.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "userId", "AttributeType": "S"},
            {"AttributeName": "createdAt", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "userId", "KeyType": "HASH"},
            {"AttributeName": "createdAt", "KeyType": "RANGE"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    now_date = int(datetime.datetime.now().timestamp() * 1000)
    table.put_item(
        Item={
            "userId": "user001",
            "createdAt": now_date,
            "address": "address1",
        }
    )
    result = table.scan()
    assert result["Count"] == 1
    assert result["Items"][0]["address"] == "address1"
    table.update_item(
        Key={"userId": "user001", "createdAt": now_date},
        UpdateExpression="SET #age=if_not_exists(#age, :initialValue)+:addOne",
        ExpressionAttributeNames={
            "#age": "age"
        },
        ExpressionAttributeValues={
            ":initialValue": 18,
            ":addOne": 1
        },
    )
    result = table.scan()
    assert result["Count"] == 1
    assert result["Items"][0]["age"] == 19


@mock_dynamodb
def test_page():
    table_name = "table1"
    dyn_resource = boto3.resource("dynamodb", region_name="ap-southeast-1")
    table = dyn_resource.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "userId", "AttributeType": "S"},
            {"AttributeName": "id", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "userId", "KeyType": "HASH"},
            {"AttributeName": "id", "KeyType": "RANGE"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(30):
        table.put_item(
            Item={
                "userId": "user001",
                "id": i,
                "address": "address{}".format(i),
            }
        )
    limit = 10
    page1_result = table.scan(ConsistentRead=True, Limit=limit)
    assert page1_result["Count"] == limit
    for i in range(10):
        assert page1_result["Items"][i]["address"] == "address{}".format(i)

    exclusive_start_key = page1_result["LastEvaluatedKey"]
    page2_result = table.scan(ConsistentRead=True, Limit=limit, ExclusiveStartKey=exclusive_start_key)
    assert page1_result["Count"] == limit
    for i in range(10):
        assert page2_result["Items"][i]["address"] == "address{}".format(10 + i)


@mock_dynamodb
def test_filter_expression():
    table_name = "table1"
    dyn_resource = boto3.resource("dynamodb", region_name="ap-southeast-1")
    table = dyn_resource.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "userPartition", "AttributeType": "S"},
            {"AttributeName": "userId", "AttributeType": "N"},
        ],
        KeySchema=[
            {"AttributeName": "userPartition", "KeyType": "HASH"},
            {"AttributeName": "userId", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    for i in range(30):
        table.put_item(
            Item={
                "userPartition": "user",
                "userId": i,
                "age": i + 15,
                "address": "address{}".format(i),
            }
        )
    result = table.query(
        KeyConditionExpression="#userPartition = :userPartition",
        FilterExpression="#age BETWEEN :min_age AND :max_age",
        ExpressionAttributeNames={
            "#userPartition": "userPartition",
            "#age": "age",
        },
        ExpressionAttributeValues={
            ":userPartition": "user",
            ":min_age": 35,
            ":max_age": 40,
        },
    )
    assert result["Count"] == 6
    assert result["Items"][0]["age"] == 35
    assert result["Items"][5]["age"] == 40

