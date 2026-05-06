package com.a2.assignment.music;

// Built based on the AWS DynamoDB Java document-model style used in RMIT COSC2626 Practical Exercise 3 sample code.

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

public class LoginService {

    private static final String TABLE_NAME = "login";

    public static void main(String[] args) {

        /*
         * This main method is only for testing.
         * Later, the backend API will call validateLogin() directly.
         */

        String email = "s41355980@gmail.com";
        String password = "012345";

        LoginResult result = validateLogin(email, password);

        if (result.isValid()) {
            System.out.println("Login successful.");
            System.out.println("Email: " + result.getEmail());
            System.out.println("Username: " + result.getUserName());
        } else {
            System.out.println("email or password is invalid");
        }
    }

    public static LoginResult validateLogin(String email, String password) {

        if (isBlank(email) || isBlank(password)) {
            return new LoginResult(false, null, null);
        }

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(TABLE_NAME);

        try {
            /*
             * The login table uses email as the partition key.
             * So we can directly get the user item using email.
             */
            Item item = table.getItem("email", email);

            if (item == null) {
                return new LoginResult(false, null, null);
            }

            String storedPassword = item.getString("password");
            String userName = item.getString("user_name");

            if (password.equals(storedPassword)) {
                return new LoginResult(true, email, userName);
            } else {
                return new LoginResult(false, null, null);
            }

        } catch (Exception e) {
            System.err.println("Error while validating login.");
            System.err.println(e.getMessage());

            return new LoginResult(false, null, null);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static class LoginResult {
        private boolean valid;
        private String email;
        private String userName;

        public LoginResult(boolean valid, String email, String userName) {
            this.valid = valid;
            this.email = email;
            this.userName = userName;
        }

        public boolean isValid() {
            return valid;
        }

        public String getEmail() {
            return email;
        }

        public String getUserName() {
            return userName;
        }
    }
}