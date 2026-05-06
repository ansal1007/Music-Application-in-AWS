package com.a2.assignment.music;

// Reference - RMIT COSC2626 Practical Exercise 3 sample code structure.

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Iterator;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class MusicLoadData {

    public static void main(String[] args) throws Exception {

        /*
         * This connects to real AWS DynamoDB, not DynamoDB Local.
         * Make sure your AWS Academy credentials are updated in your default profile.
         */
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("music");

        File jsonFile = new File("2026a2_songs.json");

        if (!jsonFile.exists()) {
            System.err.println("Cannot find 2026a2_songs.json");
            System.err.println("Place the file in the project root folder, next to pom.xml.");
            return;
        }

        JsonParser parser = new JsonFactory().createParser(jsonFile);
        JsonNode rootNode = new ObjectMapper().readTree(parser);

        /*
         * Your JSON file structure is:
         * {
         *   "songs": [
         *      { song item 1 },
         *      { song item 2 }
         *   ]
         * }
         *
         * So we must read rootNode.path("songs"), not rootNode.iterator().
         */
        JsonNode songsNode = rootNode.path("songs");
        Iterator<JsonNode> iter = songsNode.iterator();

        ObjectNode currentNode;

        int insertedCount = 0;
        int skippedCount = 0;
        int failedCount = 0;

        while (iter.hasNext()) {
            currentNode = (ObjectNode) iter.next();

            String title = currentNode.path("title").asText();
            String artist = currentNode.path("artist").asText();
            String year = currentNode.path("year").asText();
            String album = currentNode.path("album").asText();

            /*
             * The JSON file uses img_url.
             * The assignment asks for image_url.
             * So we read img_url and store it in DynamoDB as image_url.
             */
            String imageUrl = currentNode.path("img_url").asText();

            try {
                if (isBlank(title) || isBlank(artist) || isBlank(year) || isBlank(album) || isBlank(imageUrl)) {
                    throw new Exception("Missing required song field.");
                }

                /*
                 * JSON has no song_id.
                 * We generate one using artist + title + year + album.
                 * A hash avoids problems with special characters in titles such as #40 and #41.
                 */
                String songId = generateSongId(artist, title, year, album);

                /*
                 * These extra attributes support our LSI and GSI.
                 *
                 * LSI:
                 * artist-year-index
                 * PK = artist
                 * SK = year_title_album
                 *
                 * GSI:
                 * album-artist-index
                 * PK = album
                 * SK = artist_title_year
                 */
                String yearTitleAlbum = year + "#" + title + "#" + album;
                String artistTitleYear = artist + "#" + title + "#" + year;

                /*
                 * Later, when we upload images to S3, we can use this key.
                 * Example: artist-images/TaylorSwift.jpg
                 */
                String s3ImageKey = "artist-images/" + getFileNameFromUrl(imageUrl);

                Item item = new Item()
                        .withPrimaryKey("artist", artist, "song_id", songId)
                        .withString("title", title)
                        .withString("year", year)
                        .withString("album", album)
                        .withString("image_url", imageUrl)
                        .withString("s3_image_key", s3ImageKey)
                        .withString("year_title_album", yearTitleAlbum)
                        .withString("artist_title_year", artistTitleYear);

                /*
                 * This prevents accidental overwriting if the program is run again.
                 */
                PutItemSpec putItemSpec = new PutItemSpec()
                        .withItem(item)
                        .withConditionExpression("attribute_not_exists(artist) AND attribute_not_exists(song_id)");

                table.putItem(putItemSpec);

                insertedCount++;
                System.out.println("PutItem succeeded: " + artist + " - " + title);

            } catch (ConditionalCheckFailedException e) {
                skippedCount++;
                System.out.println("Skipped duplicate: " + artist + " - " + title);

            } catch (Exception e) {
                failedCount++;
                System.err.println("Unable to add song: " + artist + " - " + title);
                System.err.println(e.getMessage());
            }
        }

        parser.close();

        System.out.println();
        System.out.println("Music data import completed.");
        System.out.println("Inserted items: " + insertedCount);
        System.out.println("Skipped duplicates: " + skippedCount);
        System.out.println("Failed items: " + failedCount);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static String generateSongId(String artist, String title, String year, String album) throws Exception {
        String rawValue = artist + "|" + title + "|" + year + "|" + album;

        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] encodedHash = digest.digest(rawValue.getBytes(StandardCharsets.UTF_8));

        StringBuilder hexString = new StringBuilder();

        for (byte b : encodedHash) {
            hexString.append(String.format("%02x", b));
        }

        /*
         * We only need a shorter stable ID for this assignment dataset.
         * The original song details are still stored separately, so no data is lost.
         */
        return hexString.substring(0, 24);
    }

    private static String getFileNameFromUrl(String imageUrl) {
        int lastSlashIndex = imageUrl.lastIndexOf("/");

        if (lastSlashIndex == -1 || lastSlashIndex == imageUrl.length() - 1) {
            return "unknown-image.jpg";
        }

        return imageUrl.substring(lastSlashIndex + 1);
    }
}