package com.a2.assignment.music;

// Reference RMIT COSC2626 Practical Exercise 3 sample code.

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class LoginCreateTable {

    private static final String TABLE_NAME = "login";

    public static void main(String[] args) {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        createLoginTable(dynamoDB);
        insertLoginUsers(dynamoDB);
    }

    private static void createLoginTable(DynamoDB dynamoDB) {
        try {
            System.out.println("Creating table: " + TABLE_NAME);

            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(TABLE_NAME)

                    // Primary key: email
                    // Email must be unique, so it is the best partition key for login and registration.
                    .withKeySchema(
                            new KeySchemaElement("email", KeyType.HASH)
                    )

                    // Only key attributes need to be defined here.
                    // user_name and password are normal attributes, so they do not need to be defined here.
                    .withAttributeDefinitions(
                            new AttributeDefinition("email", ScalarAttributeType.S)
                    )

                    // On-demand capacity is simpler for this assignment.
                    .withBillingMode(BillingMode.PAY_PER_REQUEST);

            Table table = dynamoDB.createTable(request);

            System.out.println("Waiting for table to become active...");
            table.waitForActive();

            System.out.println("Table created successfully: " + TABLE_NAME);

        } catch (ResourceInUseException e) {
            System.out.println("Table already exists: " + TABLE_NAME);
            System.out.println("Continuing to insert login users...");

        } catch (Exception e) {
            System.err.println("Unable to create table: " + TABLE_NAME);
            System.err.println(e.getMessage());
        }
    }

    private static void insertLoginUsers(DynamoDB dynamoDB) {
        Table table = dynamoDB.getTable(TABLE_NAME);

        int insertedCount = 0;
        int skippedCount = 0;
        int failedCount = 0;

        /*
         * Assignment pattern:
         * email:     student id + 0 to 9
         * user_name: name + 0 to 9
         * password:  012345, 123456, ..., 901234
         *
         * User requested first email as:
         * s41355980@gmail.com
         */
        String emailPrefix = "s4135598";
        String emailDomain = "@gmail.com";
        String userNamePrefix = "GouthamSaji";

        for (int i = 0; i < 10; i++) {
            String email = emailPrefix + i + emailDomain;
            String userName = userNamePrefix + i;
            String password = generatePassword(i);

            try {
                Item item = new Item()
                        .withPrimaryKey("email", email)
                        .withString("user_name", userName)
                        .withString("password", password);

                PutItemSpec putItemSpec = new PutItemSpec()
                        .withItem(item)
                        .withConditionExpression("attribute_not_exists(email)");

                table.putItem(putItemSpec);

                insertedCount++;
                System.out.println("Inserted user: " + email + " | " + userName + " | " + password);

            } catch (ConditionalCheckFailedException e) {
                skippedCount++;
                System.out.println("Skipped existing user: " + email);

            } catch (Exception e) {
                failedCount++;
                System.err.println("Unable to insert user: " + email);
                System.err.println(e.getMessage());
            }
        }

        System.out.println();
        System.out.println("Login table setup completed.");
        System.out.println("Inserted users: " + insertedCount);
        System.out.println("Skipped users: " + skippedCount);
        System.out.println("Failed users: " + failedCount);
    }

    private static String generatePassword(int startDigit) {
        StringBuilder password = new StringBuilder();

        for (int i = 0; i < 6; i++) {
            password.append((startDigit + i) % 10);
        }

        return password.toString();
    }
}