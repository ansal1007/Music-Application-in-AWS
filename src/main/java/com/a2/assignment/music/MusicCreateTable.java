package com.a2.assignment.music;

// Reference - RMIT COSC2626 Practical Exercise 3 sample structure.

import java.util.Arrays;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

public class MusicCreateTable {

    public static void main(String[] args) {

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        String tableName = "music";

        try {
            System.out.println("Creating table: " + tableName);

            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName)

                    /*
                     * Main table key:
                     * Partition key = artist
                     * Sort key = song_id
                     */
                    .withKeySchema(
                            new KeySchemaElement("artist", KeyType.HASH),
                            new KeySchemaElement("song_id", KeyType.RANGE)
                    )

                    /*
                     * All attributes used as table/index keys must be defined here.
                     * Normal attributes like title, year, album, image_url do not need to be defined here
                     * unless they are used as key attributes.
                     */
                    .withAttributeDefinitions(Arrays.asList(
                            new AttributeDefinition("artist", ScalarAttributeType.S),
                            new AttributeDefinition("song_id", ScalarAttributeType.S),
                            new AttributeDefinition("year_title_album", ScalarAttributeType.S),
                            new AttributeDefinition("album", ScalarAttributeType.S),
                            new AttributeDefinition("artist_title_year", ScalarAttributeType.S)
                    ))

                    /*
                     * On-demand mode is simpler for the assignment.
                     * No read/write capacity numbers are needed.
                     */
                    .withBillingMode(BillingMode.PAY_PER_REQUEST)

                    /*
                     * Local Secondary Index:
                     * Same partition key as main table: artist
                     * Different sort key: year_title_album
                     *
                     * Purpose:
                     * Efficiently query songs by artist and year.
                     * Example: Jimmy Buffett in 1974
                     */
                    .withLocalSecondaryIndexes(
                            new LocalSecondaryIndex()
                                    .withIndexName("artist-year-index")
                                    .withKeySchema(
                                            new KeySchemaElement("artist", KeyType.HASH),
                                            new KeySchemaElement("year_title_album", KeyType.RANGE)
                                    )
                                    .withProjection(
                                            new Projection().withProjectionType(ProjectionType.ALL)
                                    )
                    )

                    /*
                     * Global Secondary Index:
                     * Partition key = album
                     * Sort key = artist_title_year
                     *
                     * Purpose:
                     * Query songs by album, then filter/check artist, title, or year.
                     * Example: Taylor Swift songs in album Fearless
                     */
                    .withGlobalSecondaryIndexes(
                            new GlobalSecondaryIndex()
                                    .withIndexName("album-artist-index")
                                    .withKeySchema(
                                            new KeySchemaElement("album", KeyType.HASH),
                                            new KeySchemaElement("artist_title_year", KeyType.RANGE)
                                    )
                                    .withProjection(
                                            new Projection().withProjectionType(ProjectionType.ALL)
                                    )
                    );

            Table table = dynamoDB.createTable(request);

            System.out.println("Waiting for table to become active...");
            table.waitForActive();

            System.out.println("Success. Table created and active: " + table.getDescription().getTableName());

        } catch (ResourceInUseException e) {
            System.out.println("Table already exists: " + tableName);
            System.out.println("If you need to recreate it, delete the existing table first from DynamoDB console.");

        } catch (Exception e) {
            System.err.println("Unable to create table: " + tableName);
            System.err.println(e.getMessage());
        }
    }
}