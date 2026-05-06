package com.a2.assignment.music;

// Built based on the AWS Java SDK style used in RMIT COSC2626 Practical Exercise 3 sample code.
// This program reads 2026a2_songs.json, downloads unique artist images from img_url,
// and uploads them to an S3 bucket under the artist-images/ prefix.

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class MusicImageUploadToS3 {

    private static final String BUCKET_NAME = "s4135523-music-images";

    private static final String IMAGE_FOLDER = "artist-images/";

    public static void main(String[] args) throws Exception {

        File jsonFile = new File("2026a2_songs.json");

        if (!jsonFile.exists()) {
            System.err.println("Cannot find 2026a2_songs.json");
            System.err.println("Place the JSON file in the project root folder, next to pom.xml.");
            return;
        }

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .build();

        if (!s3Client.doesBucketExistV2(BUCKET_NAME)) {
            System.err.println("Bucket does not exist: " + BUCKET_NAME);
            System.err.println("Create the bucket in S3 first, then run this program again.");
            return;
        }

        JsonParser parser = new JsonFactory().createParser(jsonFile);
        JsonNode rootNode = new ObjectMapper().readTree(parser);

        JsonNode songsNode = rootNode.path("songs");
        Iterator<JsonNode> iter = songsNode.iterator();

        /*
         * The same artist image URL appears many times in the JSON.
         * This Set prevents downloading and uploading the same image repeatedly.
         */
        Set<String> uploadedImageUrls = new HashSet<>();

        int uploadedCount = 0;
        int skippedCount = 0;
        int failedCount = 0;

        while (iter.hasNext()) {
            ObjectNode currentNode = (ObjectNode) iter.next();

            String artist = currentNode.path("artist").asText();
            String imageUrl = currentNode.path("img_url").asText();

            if (isBlank(imageUrl)) {
                failedCount++;
                System.err.println("Missing img_url for artist: " + artist);
                continue;
            }

            if (uploadedImageUrls.contains(imageUrl)) {
                skippedCount++;
                System.out.println("Skipped duplicate image: " + imageUrl);
                continue;
            }

            try {
                String fileName = getFileNameFromUrl(imageUrl);
                String s3Key = IMAGE_FOLDER + fileName;

                byte[] imageBytes = downloadImage(imageUrl);

                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(imageBytes.length);
                metadata.setContentType(getContentType(fileName));

                ByteArrayInputStream inputStream = new ByteArrayInputStream(imageBytes);

                PutObjectRequest putObjectRequest = new PutObjectRequest(
                        BUCKET_NAME,
                        s3Key,
                        inputStream,
                        metadata
                );

                s3Client.putObject(putObjectRequest);

                uploadedImageUrls.add(imageUrl);
                uploadedCount++;

                System.out.println("Uploaded: " + artist + " -> s3://" + BUCKET_NAME + "/" + s3Key);

            } catch (Exception e) {
                failedCount++;
                System.err.println("Failed to upload image for artist: " + artist);
                System.err.println("Image URL: " + imageUrl);
                System.err.println("Reason: " + e.getMessage());
            }
        }

        parser.close();

        System.out.println();
        System.out.println("Image upload completed.");
        System.out.println("Uploaded unique images: " + uploadedCount);
        System.out.println("Skipped duplicate image references: " + skippedCount);
        System.out.println("Failed images: " + failedCount);
    }

    private static byte[] downloadImage(String imageUrl) throws Exception {
        URL url = new URL(imageUrl);

        try (InputStream inputStream = url.openStream()) {
            return inputStream.readAllBytes();
        }
    }

    private static String getFileNameFromUrl(String imageUrl) {
        int lastSlashIndex = imageUrl.lastIndexOf("/");

        if (lastSlashIndex == -1 || lastSlashIndex == imageUrl.length() - 1) {
            return "unknown-image.jpg";
        }

        return imageUrl.substring(lastSlashIndex + 1);
    }

    private static String getContentType(String fileName) {
        String lowerFileName = fileName.toLowerCase();

        if (lowerFileName.endsWith(".png")) {
            return "image/png";
        } else if (lowerFileName.endsWith(".webp")) {
            return "image/webp";
        } else if (lowerFileName.endsWith(".gif")) {
            return "image/gif";
        } else {
            return "image/jpeg";
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}