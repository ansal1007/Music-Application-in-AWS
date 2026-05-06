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
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class MusicQuery {

    private static final String MUSIC_TABLE = "music";
    private static final String ARTIST_YEAR_INDEX = "artist-year-index";
    private static final String ALBUM_ARTIST_INDEX = "album-artist-index";

    private static final String IMAGE_BUCKET_NAME = "s4135598-my-artist-images";

    public static void main(String[] args) {

        /*
         * Test case 1:
         * Marker-style query: Jimmy Buffett in 1974
         */
        List<MusicItem> results = queryMusic("", "1974", "Jimmy Buffett", "");

        if (results.isEmpty()) {
            System.out.println("No result is retrieved. Please query again");
        } else {
            for (MusicItem song : results) {
                System.out.println(song);
            }
        }

        /*
         * Test case 2:
         * Taylor Swift in album Fearless
         */
        // List<MusicItem> results2 = queryMusic("", "", "Taylor Swift", "Fearless");
        // for (MusicItem song : results2) {
        //     System.out.println(song);
        // }
    }

    public static List<MusicItem> queryMusic(String title, String year, String artist, String album) {

        title = clean(title);
        year = clean(year);
        artist = clean(artist);
        album = clean(album);

        List<MusicItem> finalResults = new ArrayList<>();

        if (isBlank(title) && isBlank(year) && isBlank(artist) && isBlank(album)) {
            System.out.println("At least one query field must be entered.");
            return finalResults;
        }

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        DynamoDB dynamoDB = new DynamoDB(client);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        Table musicTable = dynamoDB.getTable(MUSIC_TABLE);

        try {
            List<MusicItem> candidateSongs;

            /*
             * DynamoDB key queries are case-sensitive.
             * So we first try efficient Query operations.
             * If no result is found, we fall back to Scan and then use equalsIgnoreCase()
             * inside matchesAllConditions().
             */

            if (!isBlank(artist) && !isBlank(year)) {
                candidateSongs = queryByArtistAndYear(musicTable, s3Client, artist, year);

                if (candidateSongs.isEmpty()) {
                    candidateSongs = scanMusicTable(musicTable, s3Client);
                }
            }

            else if (!isBlank(artist)) {
                candidateSongs = queryByArtist(musicTable, s3Client, artist);

                if (candidateSongs.isEmpty()) {
                    candidateSongs = scanMusicTable(musicTable, s3Client);
                }
            }

            else if (!isBlank(album)) {
                candidateSongs = queryByAlbum(musicTable, s3Client, album);

                if (candidateSongs.isEmpty()) {
                    candidateSongs = scanMusicTable(musicTable, s3Client);
                }
            }

            else {
                candidateSongs = scanMusicTable(musicTable, s3Client);
            }

            for (MusicItem song : candidateSongs) {
                if (matchesAllConditions(song, title, year, artist, album)) {
                    finalResults.add(song);
                }
            }

        } catch (Exception e) {
            System.err.println("Error while querying music table.");
            System.err.println(e.getMessage());
        }

        return finalResults;
    }

    private static List<MusicItem> queryByArtist(Table table, AmazonS3 s3Client, String artist) {
        List<MusicItem> results = new ArrayList<>();

        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("artist = :artist")
                .withValueMap(new ValueMap().withString(":artist", artist));

        ItemCollection<QueryOutcome> items = table.query(querySpec);
        Iterator<Item> iterator = items.iterator();

        while (iterator.hasNext()) {
            results.add(toMusicItem(iterator.next(), s3Client));
        }

        return results;
    }

    private static List<MusicItem> queryByArtistAndYear(Table table, AmazonS3 s3Client, String artist, String year) {
        List<MusicItem> results = new ArrayList<>();

        Index index = table.getIndex(ARTIST_YEAR_INDEX);

        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("artist = :artist and begins_with(year_title_album, :yearPrefix)")
                .withValueMap(new ValueMap()
                        .withString(":artist", artist)
                        .withString(":yearPrefix", year));

        ItemCollection<QueryOutcome> items = index.query(querySpec);
        Iterator<Item> iterator = items.iterator();

        while (iterator.hasNext()) {
            results.add(toMusicItem(iterator.next(), s3Client));
        }

        return results;
    }

    private static List<MusicItem> queryByAlbum(Table table, AmazonS3 s3Client, String album) {
        List<MusicItem> results = new ArrayList<>();

        Index index = table.getIndex(ALBUM_ARTIST_INDEX);

        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("album = :album")
                .withValueMap(new ValueMap().withString(":album", album));

        ItemCollection<QueryOutcome> items = index.query(querySpec);
        Iterator<Item> iterator = items.iterator();

        while (iterator.hasNext()) {
            results.add(toMusicItem(iterator.next(), s3Client));
        }

        return results;
    }

    private static List<MusicItem> scanMusicTable(Table table, AmazonS3 s3Client) {
        List<MusicItem> results = new ArrayList<>();

        ScanSpec scanSpec = new ScanSpec();

        ItemCollection<ScanOutcome> items = table.scan(scanSpec);
        Iterator<Item> iterator = items.iterator();

        while (iterator.hasNext()) {
            results.add(toMusicItem(iterator.next(), s3Client));
        }

        return results;
    }

    private static boolean matchesAllConditions(MusicItem song, String title, String year, String artist, String album) {
        if (!isBlank(title) && !title.equalsIgnoreCase(song.getTitle())) {
            return false;
        }

        if (!isBlank(year) && !year.equalsIgnoreCase(song.getYear())) {
            return false;
        }

        if (!isBlank(artist) && !artist.equalsIgnoreCase(song.getArtist())) {
            return false;
        }

        if (!isBlank(album) && !album.equalsIgnoreCase(song.getAlbum())) {
            return false;
        }

        return true;
    }

    private static MusicItem toMusicItem(Item item, AmazonS3 s3Client) {
        String songId = item.getString("song_id");
        String title = item.getString("title");
        String artist = item.getString("artist");
        String year = item.getString("year");
        String album = item.getString("album");
        String imageUrl = item.getString("image_url");
        String s3ImageKey = item.getString("s3_image_key");

        String s3ImageUrl = generatePresignedImageUrl(s3Client, s3ImageKey);

        return new MusicItem(songId, title, artist, year, album, imageUrl, s3ImageKey, s3ImageUrl);
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

    private static String clean(String value) {
        if (value == null) {
            return "";
        }

        return value.trim();
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static class MusicItem {
        private String songId;
        private String title;
        private String artist;
        private String year;
        private String album;
        private String imageUrl;
        private String s3ImageKey;
        private String s3ImageUrl;

        public MusicItem(String songId, String title, String artist, String year, String album,
                         String imageUrl, String s3ImageKey, String s3ImageUrl) {
            this.songId = songId;
            this.title = title;
            this.artist = artist;
            this.year = year;
            this.album = album;
            this.imageUrl = imageUrl;
            this.s3ImageKey = s3ImageKey;
            this.s3ImageUrl = s3ImageUrl;
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
            return "MusicItem{" +
                    "songId='" + songId + '\'' +
                    ", title='" + title + '\'' +
                    ", artist='" + artist + '\'' +
                    ", year='" + year + '\'' +
                    ", album='" + album + '\'' +
                    ", s3ImageKey='" + s3ImageKey + '\'' +
                    '}';
        }
    }
}