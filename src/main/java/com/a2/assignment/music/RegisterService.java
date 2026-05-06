package com.a2.assignment.music;

// Built based on the AWS DynamoDB Java document-model style used in RMIT COSC2626 Practical Exercise 3 sample code.

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

public class RegisterService {

    private static final String TABLE_NAME = "login";

    public static void main(String[] args) {

        /*
         * This main method is only for testing.
         * Later, the backend API will call registerUser() directly.
         */

        String email = "s41355980@gmail.com";
        String userName = "GouthamSaji";
        String password = "012345";

        RegisterResult result = registerUser(email, userName, password);

        System.out.println(result.getMessage());

        if (result.isSuccess()) {
            System.out.println("Registered email: " + result.getEmail());
            System.out.println("Registered username: " + result.getUserName());
        }
    }

    public static RegisterResult registerUser(String email, String userName, String password) {

        if (isBlank(email) || isBlank(userName) || isBlank(password)) {
            return new RegisterResult(false, "All fields are required", null, null);
        }

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(TABLE_NAME);

        try {
            /*
             * First, check if the email already exists.
             * Email is the partition key in the login table.
             */
            Item existingUser = table.getItem("email", email);

            if (existingUser != null) {
                return new RegisterResult(false, "The email already exists", null, null);
            }

            /*
             * If email does not exist, insert the new user.
             */
            Item newUser = new Item()
                    .withPrimaryKey("email", email)
                    .withString("user_name", userName)
                    .withString("password", password);

            /*
             * This condition gives extra protection.
             * Even if two users try to register the same email at the same time,
             * DynamoDB will not overwrite the existing account.
             */
            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(newUser)
                    .withConditionExpression("attribute_not_exists(email)");

            table.putItem(putItemSpec);

            return new RegisterResult(true, "Registration successful", email, userName);

        } catch (ConditionalCheckFailedException e) {
            return new RegisterResult(false, "The email already exists", null, null);

        } catch (Exception e) {
            System.err.println("Error while registering user.");
            System.err.println(e.getMessage());

            return new RegisterResult(false, "Registration failed", null, null);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static class RegisterResult {
        private boolean success;
        private String message;
        private String email;
        private String userName;

        public RegisterResult(boolean success, String message, String email, String userName) {
            this.success = success;
            this.message = message;
            this.email = email;
            this.userName = userName;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }

        public String getEmail() {
            return email;
        }

        public String getUserName() {
            return userName;
        }
    }
}