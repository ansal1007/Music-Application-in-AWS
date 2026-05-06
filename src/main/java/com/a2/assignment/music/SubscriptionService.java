package com.a2.assignment.music;

// Built based on the AWS DynamoDB Java document-model style used in RMIT COSC2626 Practical Exercise 3 sample code.

import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class SubscriptionService {

    private static final String MUSIC_TABLE = "music";
    private static final String SUBSCRIPTIONS_TABLE = "subscriptions";
    private static final String IMAGE_BUCKET_NAME = "s4135598-my-artist-images";

    public static void main(String[] args) {

        /*
         * Test this with an existing user and an existing song.
         */
        String email = "s41355980@gmail.com";

        List<MusicQuery.MusicItem> songs =
                MusicQuery.queryMusic("Love Story", "", "Taylor Swift", "Fearless");

        if (songs.isEmpty()) {
            System.out.println("No song found for testing.");
            return;
        }

        MusicQuery.MusicItem song = songs.get(0);

        ActionResult subscribeResult = subscribeToSong(email, song.getArtist(), song.getSongId());
        System.out.println(subscribeResult.getMessage());

        System.out.println();
        System.out.println("Current subscriptions for " + email + ":");

        List<SubscriptionItem> subscriptions = getSubscriptions(email);

        for (SubscriptionItem item : subscriptions) {
            System.out.println(item);
        }

        // To Remove the Subscription
        // ActionResult removeResult = removeSubscription(email, song.getSongId());
        // System.out.println(removeResult.getMessage());
    }

    public static ActionResult subscribeToSong(String email, String artist, String songId) {

        if (isBlank(email) || isBlank(artist) || isBlank(songId)) {
            return new ActionResult(false, "Missing email, artist, or song_id");
        }

        AmazonDynamoDB client = createDynamoDBClient();
        DynamoDB dynamoDB = new DynamoDB(client);

        Table musicTable = dynamoDB.getTable(MUSIC_TABLE);
        Table subscriptionsTable = dynamoDB.getTable(SUBSCRIPTIONS_TABLE);

        try {
            /*
             * First get the song from the music table.
             * We do not fully trust frontend data.
             * The backend fetches the real song item from DynamoDB.
             */
            Item song = musicTable.getItem("artist", artist, "song_id", songId);

            if (song == null) {
                return new ActionResult(false, "Song not found");
            }

            Item subscriptionItem = new Item()
                    .withPrimaryKey("email", email, "song_id", songId)
                    .withString("title", song.getString("title"))
                    .withString("artist", song.getString("artist"))
                    .withString("year", song.getString("year"))
                    .withString("album", song.getString("album"))
                    .withString("image_url", song.getString("image_url"))
                    .withString("s3_image_key", song.getString("s3_image_key"));

            /*
             * This prevents duplicate subscription for the same user and same song.
             */
            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(subscriptionItem)
                    .withConditionExpression("attribute_not_exists(email) AND attribute_not_exists(song_id)");

            subscriptionsTable.putItem(putItemSpec);

            return new ActionResult(true, "Song subscribed successfully");

        } catch (ConditionalCheckFailedException e) {
            return new ActionResult(false, "Song is already subscribed");

        } catch (Exception e) {
            System.err.println("Error while subscribing to song.");
            System.err.println(e.getMessage());

            return new ActionResult(false, "Subscription failed");
        }
    }

    public static List<SubscriptionItem> getSubscriptions(String email) {

        List<SubscriptionItem> subscriptions = new ArrayList<>();

        if (isBlank(email)) {
            return subscriptions;
        }

        AmazonDynamoDB client = createDynamoDBClient();
        DynamoDB dynamoDB = new DynamoDB(client);

        AmazonS3 s3Client = createS3Client();

        Table table = dynamoDB.getTable(SUBSCRIPTIONS_TABLE);

        try {
            /*
             * subscriptions table:
             * PK = email
             * SK = song_id
             *
             * This query returns all songs subscribed by one user.
             */
            QuerySpec querySpec = new QuerySpec()
                    .withKeyConditionExpression("email = :email")
                    .withValueMap(new ValueMap().withString(":email", email));

            ItemCollection<QueryOutcome> items = table.query(querySpec);
            Iterator<Item> iterator = items.iterator();

            while (iterator.hasNext()) {
                Item item = iterator.next();
                subscriptions.add(toSubscriptionItem(item, s3Client));
            }

        } catch (Exception e) {
            System.err.println("Error while getting subscriptions.");
            System.err.println(e.getMessage());
        }

        return subscriptions;
    }

    public static ActionResult removeSubscription(String email, String songId) {

        if (isBlank(email) || isBlank(songId)) {
            return new ActionResult(false, "Missing email or song_id");
        }

        AmazonDynamoDB client = createDynamoDBClient();
        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable(SUBSCRIPTIONS_TABLE);

        try {
            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey(new PrimaryKey("email", email, "song_id", songId))
                    .withConditionExpression("attribute_exists(email) AND attribute_exists(song_id)");

            table.deleteItem(deleteItemSpec);

            return new ActionResult(true, "Subscription removed successfully");

        } catch (ConditionalCheckFailedException e) {
            return new ActionResult(false, "Subscription not found");

        } catch (Exception e) {
            System.err.println("Error while removing subscription.");
            System.err.println(e.getMessage());

            return new ActionResult(false, "Remove subscription failed");
        }
    }

    private static SubscriptionItem toSubscriptionItem(Item item, AmazonS3 s3Client) {
        String email = item.getString("email");
        String songId = item.getString("song_id");
        String title = item.getString("title");
        String artist = item.getString("artist");
        String year = item.getString("year");
        String album = item.getString("album");
        String imageUrl = item.getString("image_url");
        String s3ImageKey = item.getString("s3_image_key");

        String s3ImageUrl = generatePresignedImageUrl(s3Client, s3ImageKey);

        return new SubscriptionItem(email, songId, title, artist, year, album, imageUrl, s3ImageKey, s3ImageUrl);
    }

    private static AmazonDynamoDB createDynamoDBClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();
    }

    private static AmazonS3 createS3Client() {
        return AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();
    }

    private static String generatePresignedImageUrl(AmazonS3 s3Client, String s3ImageKey) {
        if (isBlank(s3ImageKey)) {
            return "";
        }

        try {
            Date expiration = new Date();
            long oneHour = 1000 * 60 * 60;
            expiration.setTime(expiration.getTime() + oneHour);

            URL url = s3Client.generatePresignedUrl(IMAGE_BUCKET_NAME, s3ImageKey, expiration);
            return url.toString();

        } catch (Exception e) {
            System.err.println("Unable to generate S3 image URL for key: " + s3ImageKey);
            return "";
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static class ActionResult {
        private boolean success;
        private String message;

        public ActionResult(boolean success, String message) {
            this.success = success;
            this.message = message;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }
    }

    public static class SubscriptionItem {
        private String email;
        private String songId;
        private String title;
        private String artist;
        private String year;
        private String album;
        private String imageUrl;
        private String s3ImageKey;
        private String s3ImageUrl;

        public SubscriptionItem(String email, String songId, String title, String artist, String year,
                                String album, String imageUrl, String s3ImageKey, String s3ImageUrl) {
            this.email = email;
            this.songId = songId;
            this.title = title;
            this.artist = artist;
            this.year = year;
            this.album = album;
            this.imageUrl = imageUrl;
            this.s3ImageKey = s3ImageKey;
            this.s3ImageUrl = s3ImageUrl;
        }

        public String getEmail() {
            return email;
        }

        public String getSongId() {
            return songId;
        }

        public String getTitle() {
            return title;
        }

        public String getArtist() {
            return artist;
        }

        public String getYear() {
            return year;
        }

        public String getAlbum() {
            return album;
        }

        public String getImageUrl() {
            return imageUrl;
        }

        public String getS3ImageKey() {
            return s3ImageKey;
        }

        public String getS3ImageUrl() {
            return s3ImageUrl;
        }

        @Override
        public String toString() {
            return "SubscriptionItem{" +
                    "email='" + email + '\'' +
                    ", songId='" + songId + '\'' +
                    ", title='" + title + '\'' +
                    ", artist='" + artist + '\'' +
                    ", year='" + year + '\'' +
                    ", album='" + album + '\'' +
                    ", s3ImageKey='" + s3ImageKey + '\'' +
                    '}';
        }
    }
}