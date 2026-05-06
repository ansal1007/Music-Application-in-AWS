package com.a2.assignment.music;

// Extra CRUD service for the login table.
// Built using the same AWS SDK v1 DynamoDB document-model style used in the practical exercises.

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

public class UserCrudService {

    private static final String LOGIN_TABLE = "login";

    public static UserActionResult createUser(String email, String userName, String password) {
        RegisterService.RegisterResult result =
                RegisterService.registerUser(email, userName, password);

        return new UserActionResult(
                result.isSuccess(),
                result.getMessage(),
                result.getEmail(),
                result.getUserName()
        );
    }

    public static UserRecord getUser(String email) {
        if (isBlank(email)) {
            return null;
        }

        AmazonDynamoDB client = createDynamoDBClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(LOGIN_TABLE);

        try {
            Item item = table.getItem("email", email);

            if (item == null) {
                return null;
            }

            return new UserRecord(
                    item.getString("email"),
                    item.getString("user_name"),
                    item.getString("password")
            );

        } catch (Exception e) {
            System.err.println("Error while getting user.");
            System.err.println(e.getMessage());
            return null;
        }
    }

    public static UserActionResult updateUser(String email, String userName, String password) {
        if (isBlank(email)) {
            return new UserActionResult(false, "Email is required", null, null);
        }

        if (isBlank(userName) && isBlank(password)) {
            return new UserActionResult(false, "At least one field must be provided for update", null, null);
        }

        AmazonDynamoDB client = createDynamoDBClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(LOGIN_TABLE);

        try {
            StringBuilder updateExpression = new StringBuilder("set ");
            ValueMap valueMap = new ValueMap();

            boolean hasPreviousField = false;

            if (!isBlank(userName)) {
                updateExpression.append("user_name = :user_name");
                valueMap.withString(":user_name", userName);
                hasPreviousField = true;
            }

            if (!isBlank(password)) {
                if (hasPreviousField) {
                    updateExpression.append(", ");
                }

                updateExpression.append("password = :password");
                valueMap.withString(":password", password);
            }

            UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                    .withPrimaryKey("email", email)
                    .withUpdateExpression(updateExpression.toString())
                    .withValueMap(valueMap)
                    .withConditionExpression("attribute_exists(email)");

            table.updateItem(updateItemSpec);

            UserRecord updatedUser = getUser(email);

            return new UserActionResult(
                    true,
                    "User updated successfully",
                    updatedUser.getEmail(),
                    updatedUser.getUserName()
            );

        } catch (ConditionalCheckFailedException e) {
            return new UserActionResult(false, "User not found", null, null);

        } catch (Exception e) {
            System.err.println("Error while updating user.");
            System.err.println(e.getMessage());

            return new UserActionResult(false, "User update failed", null, null);
        }
    }

    public static UserActionResult deleteUser(String email) {
        if (isBlank(email)) {
            return new UserActionResult(false, "Email is required", null, null);
        }

        AmazonDynamoDB client = createDynamoDBClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(LOGIN_TABLE);

        try {
            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey(new PrimaryKey("email", email))
                    .withConditionExpression("attribute_exists(email)");

            table.deleteItem(deleteItemSpec);

            return new UserActionResult(true, "User deleted successfully", email, null);

        } catch (ConditionalCheckFailedException e) {
            return new UserActionResult(false, "User not found", null, null);

        } catch (Exception e) {
            System.err.println("Error while deleting user.");
            System.err.println(e.getMessage());

            return new UserActionResult(false, "User delete failed", null, null);
        }
    }

    private static AmazonDynamoDB createDynamoDBClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static class UserRecord {
        private String email;
        private String userName;
        private String password;

        public UserRecord(String email, String userName, String password) {
            this.email = email;
            this.userName = userName;
            this.password = password;
        }

        public String getEmail() {
            return email;
        }

        public String getUserName() {
            return userName;
        }

        public String getPassword() {
            return password;
        }
    }

    public static class UserActionResult {
        private boolean success;
        private String message;
        private String email;
        private String userName;

        public UserActionResult(boolean success, String message, String email, String userName) {
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