package com.springbbot.dynamodb.dynamodbaccess.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class EmployeeService {


    private static final String TABLE_NAME = "TestTable";

    @Autowired
    private DynamoDbClient dynamoDbClient;

    public void createTable(String tableName) {
        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(KeySchemaElement.builder()
                        .attributeName("id")
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("id")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(5L)
                        .writeCapacityUnits(5L)
                        .build())
                .build();

        try {
            dynamoDbClient.createTable(request);
            System.out.println("Table created: " + tableName);
        } catch (ResourceInUseException e) {
            System.out.println("Table already exists: " + tableName);
        }
    }

    public void insertItem(String id, String value) {
        PutItemRequest request = PutItemRequest.builder()
                .tableName(TABLE_NAME)
                .item(Map.of(
                        "id", AttributeValue.builder().s(id).build(),
                        "value", AttributeValue.builder().s(value).build()
                ))
                .build();

        dynamoDbClient.putItem(request);
        System.out.println("Item inserted with ID: " + id);
    }

    public List<Map<String, String>> scanTable() {
        List<Map<String, String>> allItems = new ArrayList<>();
        // Create a ScanRequest
        ScanRequest scanRequest = ScanRequest.builder()
                .tableName(TABLE_NAME)
                .build();

        // Scan the table and get the response
        ScanResponse scanResponse = dynamoDbClient.scan(scanRequest);

        // Iterate over the items and print them
        for (Map<String, AttributeValue> item : scanResponse.items()) {
            System.out.println(item);
            Map<String, String> itemAsString = convertToStringMap(item);
            allItems.add(itemAsString);
        }
        return allItems;
    }

    // Method to retrieve an item by ID
    public Map<String, String> findById(String id) {
        // Build the key to look for in the DynamoDB table
        Map<String, AttributeValue> keyId = Map.of("id", AttributeValue.builder().s(id).build());

        // Create the GetItemRequest
        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(TABLE_NAME)
                .key(keyId)
                .build();

        // Get the item from DynamoDB
        GetItemResponse response = dynamoDbClient.getItem(getItemRequest);

        // Check if the item is present and convert it to a String Map
        if (response.hasItem()) {
            return convertToStringMap(response.item());
        } else {
            return null; // Return null if the item is not found
        }
    }

    private Map<String, String> convertToStringMap(Map<String, AttributeValue> item) {
        Map<String, String> itemAsString = new java.util.HashMap<>();
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            itemAsString.put(entry.getKey(), entry.getValue().s());  // Converting AttributeValue to String
        }
        return itemAsString;
    }

    // Helper method to convert DynamoDB AttributeValue to String map
    private Map<String, String> convertToStringMapByType(Map<String, AttributeValue> item) {
        Map<String, String> itemAsString = new java.util.HashMap<>();
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            if (entry.getValue().s() != null) {
                itemAsString.put(entry.getKey(), entry.getValue().s());  // String value
            } else if (entry.getValue().n() != null) {
                itemAsString.put(entry.getKey(), entry.getValue().n());  // Number value
            } else {
                itemAsString.put(entry.getKey(), entry.getValue().toString());  // Default to string representation
            }
        }
        return itemAsString;
    }

    // Delete by ID
    public boolean deleteById(String id) {
        try {
            // Prepare the key (make sure this matches your table's partition key name)
            Map<String, AttributeValue> key = Map.of(
                    "id", AttributeValue.builder().s(id).build() // partition key: ID
            );

            // Create delete item request
            DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(key)
                    .build();

            // Send delete request to DynamoDB
            DeleteItemResponse deleteItemResponse = dynamoDbClient.deleteItem(deleteItemRequest);

            // If the item was deleted successfully, the response will not contain any errors
            return deleteItemResponse.sdkHttpResponse().isSuccessful();
        } catch (Exception e) {
            // Handle any exceptions (e.g., item not found)
            System.out.println("Error deleting item: " + e.getMessage());
            return false;
        }
    }

    // Update Item
    public boolean updateItem(String id, String value) {
        try {
            // Prepare the key and update expressions
            Map<String, AttributeValue> key = Map.of(
                    "id", AttributeValue.builder().s(id).build()
            );

            Map<String, AttributeValueUpdate> updatedValues = Map.of(
                    "value", AttributeValueUpdate.builder()
                            .value(AttributeValue.builder().s(value).build())
                            .action(AttributeAction.PUT)
                            .build()
            );

            // Create update item request
            UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(key)
                    .attributeUpdates(updatedValues)
                    .build();

            // Send the update request
            dynamoDbClient.updateItem(updateItemRequest);
            return true;
        } catch (Exception e) {
            // Handle any exceptions
            System.out.println("Error updating item: " + e.getMessage());
            return false;
        }
    }

    public String deleteTable(String tableName) {
        try {
            // Create a DeleteTable request
            DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
                    .tableName(tableName)
                    .build();

            // Call the deleteTable API
            DeleteTableResponse response = dynamoDbClient.deleteTable(deleteTableRequest);

            return "Table '" + tableName + "' deleted successfully. Status: " + response.sdkHttpResponse().statusCode();
        } catch (Exception e) {
            return "Error deleting table '" + tableName + "': " + e.getMessage();
        }
    }
}
