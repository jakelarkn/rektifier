package com.rektifier.smoke;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Smoke test for AWS SDK for Java v2 against rektifier. Same shape as
 * the v1 smoketest in `smoketests/java-sdk-v1/` — the two together
 * confirm that both major Java SDK generations parse rektifier's wire
 * responses and error envelopes correctly.
 */
public class Main {
    private static final String ENDPOINT =
        System.getenv().getOrDefault("REKTIFIER_URL", "http://localhost:9000");
    private static final String REGION =
        System.getenv().getOrDefault("REKTIFIER_REGION", "us-east-1");

    private static final String RUN_TAG =
        "smoke-v2-" + System.currentTimeMillis();
    private static final String USER_PK = RUN_TAG + "-user";
    private static final String QUERY_PK = RUN_TAG + "-events";

    private static final AtomicInteger pass = new AtomicInteger();
    private static final AtomicInteger fail = new AtomicInteger();

    public static void main(String[] args) {
        try (DynamoDbClient ddb = DynamoDbClient.builder()
                .endpointOverride(URI.create(ENDPOINT))
                .region(Region.of(REGION))
                .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("local", "local")))
                .build()) {

            System.out.println("=== rektifier-smoke-v2 ===");
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

            try { cleanup(ddb); } catch (Exception e) {
                System.err.println("cleanup warning: " + e.getMessage());
            }

            System.out.println();
            System.out.printf("=== %d passed, %d failed ===%n",
                pass.get(), fail.get());
            System.exit(fail.get() == 0 ? 0 : 1);
        }
    }

    // ===== Individual checks ================================================

    private static void putItem(DynamoDbClient ddb) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.fromS(USER_PK));
        item.put("label", AttributeValue.fromS("alice"));
        item.put("counter", AttributeValue.fromN("0"));
        ddb.putItem(PutItemRequest.builder()
            .tableName("users").item(item).build());
    }

    private static void getItem(DynamoDbClient ddb) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.fromS(USER_PK));
        GetItemResponse r = ddb.getItem(GetItemRequest.builder()
            .tableName("users").key(key).build());
        require(r.hasItem(), "GetItem returned no item");
        require("alice".equals(r.item().get("label").s()),
            "label != alice");
    }

    private static void updateItem(DynamoDbClient ddb) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.fromS(USER_PK));
        Map<String, String> names = new HashMap<>();
        names.put("#c", "counter");
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":inc", AttributeValue.fromN("5"));
        ddb.updateItem(UpdateItemRequest.builder()
            .tableName("users")
            .key(key)
            .updateExpression("SET #c = #c + :inc")
            .expressionAttributeNames(names)
            .expressionAttributeValues(values)
            .build());
    }

    private static void getItemPostUpdate(DynamoDbClient ddb) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.fromS(USER_PK));
        GetItemResponse r = ddb.getItem(GetItemRequest.builder()
            .tableName("users").key(key).build());
        require("5".equals(r.item().get("counter").n()),
            "counter != 5 after SET-arithmetic");
    }

    private static void deleteItem(DynamoDbClient ddb) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.fromS(USER_PK));
        ddb.deleteItem(DeleteItemRequest.builder()
            .tableName("users").key(key).build());

        GetItemResponse r = ddb.getItem(GetItemRequest.builder()
            .tableName("users").key(key).build());
        require(!r.hasItem(), "row still present after DeleteItem");
    }

    private static void querySeedAndRead(DynamoDbClient ddb) {
        for (int ts = 1; ts <= 5; ts++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("device_id", AttributeValue.fromS(QUERY_PK));
            item.put("ts", AttributeValue.fromN(String.valueOf(ts)));
            item.put("flag", AttributeValue.fromS(ts % 2 == 0 ? "on" : "off"));
            ddb.putItem(PutItemRequest.builder()
                .tableName("device_events").item(item).build());
        }
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":pk", AttributeValue.fromS(QUERY_PK));
        QueryResponse r = ddb.query(QueryRequest.builder()
            .tableName("device_events")
            .keyConditionExpression("device_id = :pk")
            .expressionAttributeValues(eav)
            .build());
        require(r.count() == 5,
            "Query count = " + r.count() + " (expected 5)");
        require(r.items().get(0).get("ts").n().equals("1"),
            "Query items not sorted by sk ASC");
    }

    private static void queryFiltered(DynamoDbClient ddb) {
        Map<String, AttributeValue> eav = new HashMap<>();
        eav.put(":pk", AttributeValue.fromS(QUERY_PK));
        eav.put(":on", AttributeValue.fromS("on"));
        QueryResponse r = ddb.query(QueryRequest.builder()
            .tableName("device_events")
            .keyConditionExpression("device_id = :pk")
            .filterExpression("flag = :on")
            .expressionAttributeValues(eav)
            .build());
        require(r.scannedCount() == 5,
            "ScannedCount = " + r.scannedCount() + " (expected 5)");
        require(r.count() == 2,
            "Filtered Count = " + r.count() + " (expected 2 'on' rows)");
    }

    private static void scanLimit(DynamoDbClient ddb) {
        ScanResponse r = ddb.scan(ScanRequest.builder()
            .tableName("device_events").limit(2).build());
        require(r.count() >= 1, "Scan with Limit=2 returned 0 items");
        require(r.hasLastEvaluatedKey() && !r.lastEvaluatedKey().isEmpty(),
            "Scan with Limit=2 should have LastEvaluatedKey");
    }

    private static void conditionalPutFails(DynamoDbClient ddb) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.fromS(USER_PK + "-ccfe"));
        ddb.putItem(PutItemRequest.builder()
            .tableName("users").item(item).build());
        try {
            ddb.putItem(PutItemRequest.builder()
                .tableName("users")
                .item(item)
                .conditionExpression("attribute_not_exists(id)")
                .build());
            throw new AssertionError("expected ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException expected) {
            // Good.
        }
    }

    private static void cleanup(DynamoDbClient ddb) {
        for (String suffix : List.of("", "-ccfe")) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("id", AttributeValue.fromS(USER_PK + suffix));
            ddb.deleteItem(DeleteItemRequest.builder()
                .tableName("users").key(key).build());
        }
        for (int ts = 1; ts <= 5; ts++) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("device_id", AttributeValue.fromS(QUERY_PK));
            key.put("ts", AttributeValue.fromN(String.valueOf(ts)));
            ddb.deleteItem(DeleteItemRequest.builder()
                .tableName("device_events").key(key).build());
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
