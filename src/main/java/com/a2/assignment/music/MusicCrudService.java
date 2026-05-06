package com.a2.assignment.music;

// Extra CRUD service for the music table.
// Built using the same AWS SDK v1 DynamoDB document-model style used in the practical exercises.

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;

public class MusicCrudService {

    private static final String MUSIC_TABLE = "music";

    public static MusicActionResult createMusic(String title, String artist, String year, String album, String imageUrl) {
        if (isBlank(title) || isBlank(artist) || isBlank(year) || isBlank(album) || isBlank(imageUrl)) {
            return new MusicActionResult(false, "All music fields are required", null, null);
        }

        AmazonDynamoDB client = createDynamoDBClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(MUSIC_TABLE);

        try {
            String songId = generateSongId(artist, title, year, album);
            String yearTitleAlbum = year + "#" + title + "#" + album;
            String artistTitleYear = artist + "#" + title + "#" + year;
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

            PutItemSpec putItemSpec = new PutItemSpec()
                    .withItem(item)
                    .withConditionExpression("attribute_not_exists(artist) AND attribute_not_exists(song_id)");

            table.putItem(putItemSpec);

            return new MusicActionResult(true, "Music item created successfully", artist, songId);

        } catch (ConditionalCheckFailedException e) {
            return new MusicActionResult(false, "Music item already exists", null, null);

        } catch (Exception e) {
            System.err.println("Error while creating music item.");
            System.err.println(e.getMessage());

            return new MusicActionResult(false, "Music item creation failed", null, null);
        }
    }

    public static MusicRecord getMusic(String artist, String songId) {
        if (isBlank(artist) || isBlank(songId)) {
            return null;
        }

        AmazonDynamoDB client = createDynamoDBClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(MUSIC_TABLE);

        try {
            Item item = table.getItem("artist", artist, "song_id", songId);

            if (item == null) {
                return null;
            }

            return toMusicRecord(item);

        } catch (Exception e) {
            System.err.println("Error while getting music item.");
            System.err.println(e.getMessage());

            return null;
        }
    }

    public static MusicActionResult updateMusic(
            String artist,
            String songId,
            String title,
            String year,
            String album,
            String imageUrl
    ) {
        if (isBlank(artist) || isBlank(songId)) {
            return new MusicActionResult(false, "artist and song_id are required", null, null);
        }

        MusicRecord existing = getMusic(artist, songId);

        if (existing == null) {
            return new MusicActionResult(false, "Music item not found", null, null);
        }

        String finalTitle = isBlank(title) ? existing.getTitle() : title;
        String finalYear = isBlank(year) ? existing.getYear() : year;
        String finalAlbum = isBlank(album) ? existing.getAlbum() : album;
        String finalImageUrl = isBlank(imageUrl) ? existing.getImageUrl() : imageUrl;

        String finalS3ImageKey = isBlank(imageUrl)
                ? existing.getS3ImageKey()
                : "artist-images/" + getFileNameFromUrl(finalImageUrl);

        String finalYearTitleAlbum = finalYear + "#" + finalTitle + "#" + finalAlbum;
        String finalArtistTitleYear = artist + "#" + finalTitle + "#" + finalYear;

        AmazonDynamoDB client = createDynamoDBClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(MUSIC_TABLE);

        try {
            UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                    .withPrimaryKey("artist", artist, "song_id", songId)
                    .withUpdateExpression(
                            "set title = :title, " +
                                    "#yr = :year, " +
                                    "album = :album, " +
                                    "image_url = :image_url, " +
                                    "s3_image_key = :s3_image_key, " +
                                    "year_title_album = :year_title_album, " +
                                    "artist_title_year = :artist_title_year"
                    )
                    .withNameMap(new com.amazonaws.services.dynamodbv2.document.utils.NameMap()
                            .with("#yr", "year")
                    )
                    .withValueMap(new ValueMap()
                            .withString(":title", finalTitle)
                            .withString(":year", finalYear)
                            .withString(":album", finalAlbum)
                            .withString(":image_url", finalImageUrl)
                            .withString(":s3_image_key", finalS3ImageKey)
                            .withString(":year_title_album", finalYearTitleAlbum)
                            .withString(":artist_title_year", finalArtistTitleYear)
                    )
                    .withConditionExpression("attribute_exists(artist) AND attribute_exists(song_id)");

            table.updateItem(updateItemSpec);

            return new MusicActionResult(true, "Music item updated successfully", artist, songId);

        } catch (ConditionalCheckFailedException e) {
            return new MusicActionResult(false, "Music item not found", null, null);

        } catch (Exception e) {
            System.err.println("Error while updating music item.");
            System.err.println(e.getMessage());

            return new MusicActionResult(false, "Music item update failed", null, null);
        }
    }

    public static MusicActionResult deleteMusic(String artist, String songId) {
        if (isBlank(artist) || isBlank(songId)) {
            return new MusicActionResult(false, "artist and song_id are required", null, null);
        }

        AmazonDynamoDB client = createDynamoDBClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(MUSIC_TABLE);

        try {
            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey(new PrimaryKey("artist", artist, "song_id", songId))
                    .withConditionExpression("attribute_exists(artist) AND attribute_exists(song_id)");

            table.deleteItem(deleteItemSpec);

            return new MusicActionResult(true, "Music item deleted successfully", artist, songId);

        } catch (ConditionalCheckFailedException e) {
            return new MusicActionResult(false, "Music item not found", null, null);

        } catch (Exception e) {
            System.err.println("Error while deleting music item.");
            System.err.println(e.getMessage());

            return new MusicActionResult(false, "Music item delete failed", null, null);
        }
    }

    private static MusicRecord toMusicRecord(Item item) {
        return new MusicRecord(
                item.getString("artist"),
                item.getString("song_id"),
                item.getString("title"),
                item.getString("year"),
                item.getString("album"),
                item.getString("image_url"),
                item.getString("s3_image_key"),
                item.getString("year_title_album"),
                item.getString("artist_title_year")
        );
    }

    private static AmazonDynamoDB createDynamoDBClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();
    }

    private static String generateSongId(String artist, String title, String year, String album) throws Exception {
        String rawValue = artist + "|" + title + "|" + year + "|" + album;

        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] encodedHash = digest.digest(rawValue.getBytes(StandardCharsets.UTF_8));

        StringBuilder hexString = new StringBuilder();

        for (byte b : encodedHash) {
            hexString.append(String.format("%02x", b));
        }

        return hexString.substring(0, 24);
    }

    private static String getFileNameFromUrl(String imageUrl) {
        int lastSlashIndex = imageUrl.lastIndexOf("/");

        if (lastSlashIndex == -1 || lastSlashIndex == imageUrl.length() - 1) {
            return "unknown-image.jpg";
        }

        return imageUrl.substring(lastSlashIndex + 1);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static class MusicRecord {
        private String artist;
        private String songId;
        private String title;
        private String year;
        private String album;
        private String imageUrl;
        private String s3ImageKey;
        private String yearTitleAlbum;
        private String artistTitleYear;

        public MusicRecord(
                String artist,
                String songId,
                String title,
                String year,
                String album,
                String imageUrl,
                String s3ImageKey,
                String yearTitleAlbum,
                String artistTitleYear
        ) {
            this.artist = artist;
            this.songId = songId;
            this.title = title;
            this.year = year;
            this.album = album;
            this.imageUrl = imageUrl;
            this.s3ImageKey = s3ImageKey;
            this.yearTitleAlbum = yearTitleAlbum;
            this.artistTitleYear = artistTitleYear;
        }

        public String getArtist() {
            return artist;
        }

        public String getSongId() {
            return songId;
        }

        public String getTitle() {
            return title;
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

        public String getYearTitleAlbum() {
            return yearTitleAlbum;
        }

        public String getArtistTitleYear() {
            return artistTitleYear;
        }
    }

    public static class MusicActionResult {
        private boolean success;
        private String message;
        private String artist;
        private String songId;

        public MusicActionResult(boolean success, String message, String artist, String songId) {
            this.success = success;
            this.message = message;
            this.artist = artist;
            this.songId = songId;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }

        public String getArtist() {
            return artist;
        }

        public String getSongId() {
            return songId;
        }
    }
}