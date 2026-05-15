package com.rektifier.clienttest;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client test for AWS SDK for Java v1 against rektifier. Confirms
 * that a real SDK client can connect to rektifier and round-trip
 * every end-to-end-supported op. Run after `just bootstrap-pg` and
 * with rektifier listening on :9000.
 *
 * Exits non-zero on any failure. Designed to be loud about which
 * specific op broke so divergences surface immediately.
 */
public class Main {
    private static final String ENDPOINT =
        System.getenv().getOrDefault("REKTIFIER_URL", "http://localhost:9000");
    private static final String REGION =
        System.getenv().getOrDefault("REKTIFIER_REGION", "us-east-1");

    // Unique-per-run PK so re-runs don't trip over each other.
    private static final String RUN_TAG =
        "client-v1-" + System.currentTimeMillis();
    private static final String USER_PK = RUN_TAG + "-user";
    private static final String QUERY_PK = RUN_TAG + "-events";

    private static final AtomicInteger pass = new AtomicInteger();
    private static final AtomicInteger fail = new AtomicInteger();

    public static void main(String[] args) {
        AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(ENDPOINT, REGION))
            .withCredentials(new AWSStaticCredentialsProvider(
                new BasicAWSCredentials("local", "local")))
            .build();

        System.out.println("=== rektifier-client-v1 ===");
        System.out.println("endpoint = " + ENDPOINT);
        System.out.println("region   = " + REGION);
        System.out.println();

        check("PutItem", () -> putItem(ddb));
        check("GetItem", () -> getItem(ddb));
        check("UpdateItem", () -> updateItem(ddb));
        check("GetItem (post-update)", () -> getItemPostUpdate(ddb));
        check("DeleteItem", () -> deleteItem(ddb));
        check("Query (composite + KCE)", () -> querySeedAndRead(ddb));
        check("Query with FilterExpression", () -> queryFiltered(ddb));
        check("Scan with Limit", () -> scanLimit(ddb));
        check("Conditional Put rejection", () -> conditionalPutFails(ddb));

        // Best-effort cleanup; failures here don't count.
        try { cleanup(ddb); } catch (Exception e) {
            System.err.println("cleanup warning: " + e.getMessage());
        }

        System.out.println();
        System.out.printf("=== %d passed, %d failed ===%n",
            pass.get(), fail.get());
        System.exit(fail.get() == 0 ? 0 : 1);
    }

    // ===== Individual checks ================================================

    private static void putItem(AmazonDynamoDB ddb) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS(USER_PK));
        item.put("label", new AttributeValue().withS("alice"));
        item.put("counter", new AttributeValue().withN("0"));
        ddb.putItem(new PutItemRequest()
            .withTableName("users")
            .withItem(item));
    }

    private static void getItem(AmazonDynamoDB ddb) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS(USER_PK));
        GetItemResult r = ddb.getItem(new GetItemRequest()
            .withTableName("users").withKey(key));
        require(r.getItem() != null, "GetItem returned no item");
        require("alice".equals(r.getItem().get("label").getS()),
            "label != alice");
    }

    private static void updateItem(AmazonDynamoDB ddb) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS(USER_PK));
        Map<String, String> names = new HashMap<>();
        names.put("#c", "counter");
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":inc", new AttributeValue().withN("5"));
        ddb.updateItem(new UpdateItemRequest()
            .withTableName("users")
            .withKey(key)
            .withUpdateExpression("SET #c = #c + :inc")
            .withExpressionAttributeNames(names)
            .withExpressionAttributeValues(values));
    }

    private static void getItemPostUpdate(AmazonDynamoDB ddb) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS(USER_PK));
        GetItemResult r = ddb.getItem(new GetItemRequest()
            .withTableName("users").withKey(key));
        require("5".equals(r.getItem().get("counter").getN()),
            "counter != 5 after SET-arithmetic");
    }

    private static void deleteItem(AmazonDynamoDB ddb) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", new AttributeValue().withS(USER_PK));
        ddb.deleteItem(new DeleteItemRequest()
            .withTableName("users").withKey(key));

        GetItemResult r = ddb.getItem(new GetItemRequest()
            .withTableName("users").withKey(key));
        require(r.getItem() == null, "row still present after DeleteItem");
    }

    private static void querySeedAndRead(AmazonDynamoDB ddb) {
        // Seed 5 rows under one partition on device_events.
        for (int ts = 1; ts <= 5; ts++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("device_id", new AttributeValue().withS(QUERY_PK));
            item.put("ts", new AttributeValue().withN(String.valueOf(ts)));
            item.put("flag", new AttributeValue()
                .withS(ts % 2 == 0 ? "on" : "off"));
            ddb.putItem(new PutItemRequest()
                .withTableName("device_events").withItem(item));
        }
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":pk", new AttributeValue().withS(QUERY_PK));
        QueryResult r = ddb.query(new QueryRequest()
            .withTableName("device_events")
            .withKeyConditionExpression("device_id = :pk")
            .withExpressionAttributeValues(eav));
        require(r.getCount() == 5,
            "Query count = " + r.getCount() + " (expected 5)");
        require(r.getItems().get(0).get("ts").getN().equals("1"),
            "Query items not sorted by sk ASC");
    }

    private static void queryFiltered(AmazonDynamoDB ddb) {
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":pk", new AttributeValue().withS(QUERY_PK));
        eav.put(":on", new AttributeValue().withS("on"));
        QueryResult r = ddb.query(new QueryRequest()
            .withTableName("device_events")
            .withKeyConditionExpression("device_id = :pk")
            .withFilterExpression("flag = :on")
            .withExpressionAttributeValues(eav));
        require(r.getScannedCount() == 5,
            "ScannedCount = " + r.getScannedCount() + " (expected 5)");
        require(r.getCount() == 2,
            "Filtered Count = " + r.getCount() + " (expected 2 'on' rows)");
    }

    private static void scanLimit(AmazonDynamoDB ddb) {
        ScanResult r = ddb.scan(new ScanRequest()
            .withTableName("device_events")
            .withLimit(2));
        require(r.getCount() >= 1,
            "Scan with Limit=2 returned 0 items");
        // LEK should be present when scan caps at Limit (DDB semantic
        // pinned in PLAN-4 D / commit 73323bd).
        require(r.getLastEvaluatedKey() != null
                && !r.getLastEvaluatedKey().isEmpty(),
            "Scan with Limit=2 should have LastEvaluatedKey");
    }

    private static void conditionalPutFails(AmazonDynamoDB ddb) {
        // Insert a fresh row, then try to re-insert with
        // attribute_not_exists(id) and confirm we get the right
        // exception class through the SDK.
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", new AttributeValue().withS(USER_PK + "-ccfe"));
        ddb.putItem(new PutItemRequest()
            .withTableName("users").withItem(item));

        try {
            ddb.putItem(new PutItemRequest()
                .withTableName("users")
                .withItem(item)
                .withConditionExpression("attribute_not_exists(id)"));
            throw new AssertionError("expected ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException expected) {
            // Good: the SDK parsed the error wire shape correctly.
        }
    }

    private static void cleanup(AmazonDynamoDB ddb) {
        // users
        for (String suffix : List.of("", "-ccfe")) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("id", new AttributeValue().withS(USER_PK + suffix));
            ddb.deleteItem(new DeleteItemRequest()
                .withTableName("users").withKey(key));
        }
        // device_events partition
        for (int ts = 1; ts <= 5; ts++) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("device_id", new AttributeValue().withS(QUERY_PK));
            key.put("ts", new AttributeValue().withN(String.valueOf(ts)));
            ddb.deleteItem(new DeleteItemRequest()
                .withTableName("device_events").withKey(key));
        }
    }

    // ===== Harness ==========================================================

    private static void check(String name, Runnable body) {
        try {
            body.run();
            System.out.println("  PASS  " + name);
            pass.incrementAndGet();
        } catch (Throwable t) {
            System.out.println("  FAIL  " + name + " — " + t);
            fail.incrementAndGet();
        }
    }

    private static void require(boolean cond, String msg) {
        if (!cond) throw new AssertionError(msg);
    }
}
