package com.a2.assignment.music;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class MusicApiController {

    @GetMapping("/health")
    public Map<String, Object> healthCheck() {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", true);
        response.put("message", "Music API server is running");
        return response;
    }

    @PostMapping("/login")
    public Map<String, Object> login(@RequestBody LoginRequest request) {
        LoginService.LoginResult result =
                LoginService.validateLogin(request.getEmail(), request.getPassword());

        Map<String, Object> response = new LinkedHashMap<>();

        if (result.isValid()) {
            response.put("success", true);
            response.put("message", "Login successful");
            response.put("email", result.getEmail());
            response.put("user_name", result.getUserName());
        } else {
            response.put("success", false);
            response.put("message", "email or password is invalid");
        }

        return response;
    }

    @PostMapping("/register")
    public Map<String, Object> register(@RequestBody RegisterRequest request) {
        RegisterService.RegisterResult result =
                RegisterService.registerUser(
                        request.getEmail(),
                        request.getUserName(),
                        request.getPassword()
                );

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", result.isSuccess());
        response.put("message", result.getMessage());

        if (result.isSuccess()) {
            response.put("email", result.getEmail());
            response.put("user_name", result.getUserName());
        }

        return response;
    }

    @GetMapping("/music/query")
    public Map<String, Object> queryMusic(
            @RequestParam(required = false, defaultValue = "") String title,
            @RequestParam(required = false, defaultValue = "") String year,
            @RequestParam(required = false, defaultValue = "") String artist,
            @RequestParam(required = false, defaultValue = "") String album
    ) {
        List<MusicQuery.MusicItem> songs =
                MusicQuery.queryMusic(title, year, artist, album);

        List<Map<String, Object>> resultList = new ArrayList<>();

        for (MusicQuery.MusicItem song : songs) {
            resultList.add(convertMusicItemToMap(song));
        }

        Map<String, Object> response = new LinkedHashMap<>();

        if (resultList.isEmpty()) {
            response.put("success", false);
            response.put("message", "No result is retrieved. Please query again");
            response.put("songs", resultList);
        } else {
            response.put("success", true);
            response.put("message", "Music results retrieved successfully");
            response.put("songs", resultList);
        }

        return response;
    }

    @GetMapping("/subscriptions")
    public Map<String, Object> getSubscriptions(
            @RequestParam String email
    ) {
        List<SubscriptionService.SubscriptionItem> subscriptions =
                SubscriptionService.getSubscriptions(email);

        List<Map<String, Object>> resultList = new ArrayList<>();

        for (SubscriptionService.SubscriptionItem item : subscriptions) {
            resultList.add(convertSubscriptionItemToMap(item));
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", true);
        response.put("email", email);
        response.put("subscriptions", resultList);

        return response;
    }

    @PostMapping("/subscriptions")
    public Map<String, Object> subscribeToSong(@RequestBody SubscribeRequest request) {
        SubscriptionService.ActionResult result =
                SubscriptionService.subscribeToSong(
                        request.getEmail(),
                        request.getArtist(),
                        request.getSongId()
                );

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", result.isSuccess());
        response.put("message", result.getMessage());

        return response;
    }

    @DeleteMapping("/subscriptions")
    public Map<String, Object> removeSubscription(
            @RequestParam String email,
            @RequestParam("song_id") String songId
    ) {
        SubscriptionService.ActionResult result =
                SubscriptionService.removeSubscription(email, songId);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", result.isSuccess());
        response.put("message", result.getMessage());

        return response;
    }

    private Map<String, Object> convertMusicItemToMap(MusicQuery.MusicItem song) {
        Map<String, Object> map = new LinkedHashMap<>();

        map.put("song_id", song.getSongId());
        map.put("title", song.getTitle());
        map.put("artist", song.getArtist());
        map.put("year", song.getYear());
        map.put("album", song.getAlbum());
        map.put("image_url", song.getImageUrl());
        map.put("s3_image_key", song.getS3ImageKey());
        map.put("s3_image_url", song.getS3ImageUrl());

        return map;
    }

    private Map<String, Object> convertSubscriptionItemToMap(SubscriptionService.SubscriptionItem item) {
        Map<String, Object> map = new LinkedHashMap<>();

        map.put("email", item.getEmail());
        map.put("song_id", item.getSongId());
        map.put("title", item.getTitle());
        map.put("artist", item.getArtist());
        map.put("year", item.getYear());
        map.put("album", item.getAlbum());
        map.put("image_url", item.getImageUrl());
        map.put("s3_image_key", item.getS3ImageKey());
        map.put("s3_image_url", item.getS3ImageUrl());

        return map;
    }

    public static class LoginRequest {
        private String email;
        private String password;

        public LoginRequest() {
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public static class RegisterRequest {
        private String email;

        @JsonProperty("user_name")
        private String userName;

        private String password;

        public RegisterRequest() {
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public static class SubscribeRequest {
        private String email;
        private String artist;

        @JsonProperty("song_id")
        private String songId;

        public SubscribeRequest() {
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public String getArtist() {
            return artist;
        }

        public void setArtist(String artist) {
            this.artist = artist;
        }

        public String getSongId() {
            return songId;
        }

        public void setSongId(String songId) {
            this.songId = songId;
        }
    }

    @GetMapping("/users/{email}")
    public Map<String, Object> getUser(@PathVariable String email) {
        UserCrudService.UserRecord user = UserCrudService.getUser(email);

        Map<String, Object> response = new LinkedHashMap<>();

        if (user == null) {
            response.put("success", false);
            response.put("message", "User not found");
        } else {
            response.put("success", true);
            response.put("email", user.getEmail());
            response.put("user_name", user.getUserName());

            /*
             * Password is intentionally not returned in the API response.
             * It exists in DynamoDB only because the assignment allows plain text for simplicity.
             */
        }

        return response;
    }

    @PutMapping("/users/{email}")
    public Map<String, Object> updateUser(
            @PathVariable String email,
            @RequestBody UpdateUserRequest request
    ) {
        UserCrudService.UserActionResult result =
                UserCrudService.updateUser(email, request.getUserName(), request.getPassword());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", result.isSuccess());
        response.put("message", result.getMessage());

        if (result.isSuccess()) {
            response.put("email", result.getEmail());
            response.put("user_name", result.getUserName());
        }

        return response;
    }

    @DeleteMapping("/users/{email}")
    public Map<String, Object> deleteUser(@PathVariable String email) {
        UserCrudService.UserActionResult result =
                UserCrudService.deleteUser(email);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", result.isSuccess());
        response.put("message", result.getMessage());

        return response;
    }

    @PostMapping("/music")
    public Map<String, Object> createMusic(@RequestBody CreateMusicRequest request) {
        MusicCrudService.MusicActionResult result =
                MusicCrudService.createMusic(
                        request.getTitle(),
                        request.getArtist(),
                        request.getYear(),
                        request.getAlbum(),
                        request.getImageUrl()
                );

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", result.isSuccess());
        response.put("message", result.getMessage());

        if (result.isSuccess()) {
            response.put("artist", result.getArtist());
            response.put("song_id", result.getSongId());
        }

        return response;
    }

    @GetMapping("/music/item")
    public Map<String, Object> getMusicItem(
            @RequestParam String artist,
            @RequestParam("song_id") String songId
    ) {
        MusicCrudService.MusicRecord item = MusicCrudService.getMusic(artist, songId);

        Map<String, Object> response = new LinkedHashMap<>();

        if (item == null) {
            response.put("success", false);
            response.put("message", "Music item not found");
        } else {
            response.put("success", true);
            response.put("music", convertMusicRecordToMap(item));
        }

        return response;
    }

    @PutMapping("/music/item")
    public Map<String, Object> updateMusicItem(
            @RequestParam String artist,
            @RequestParam("song_id") String songId,
            @RequestBody UpdateMusicRequest request
    ) {
        MusicCrudService.MusicActionResult result =
                MusicCrudService.updateMusic(
                        artist,
                        songId,
                        request.getTitle(),
                        request.getYear(),
                        request.getAlbum(),
                        request.getImageUrl()
                );

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", result.isSuccess());
        response.put("message", result.getMessage());

        if (result.isSuccess()) {
            response.put("artist", result.getArtist());
            response.put("song_id", result.getSongId());
        }

        return response;
    }

    @DeleteMapping("/music/item")
    public Map<String, Object> deleteMusicItem(
            @RequestParam String artist,
            @RequestParam("song_id") String songId
    ) {
        MusicCrudService.MusicActionResult result =
                MusicCrudService.deleteMusic(artist, songId);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("success", result.isSuccess());
        response.put("message", result.getMessage());

        return response;
    }

    private Map<String, Object> convertMusicRecordToMap(MusicCrudService.MusicRecord item) {
        Map<String, Object> map = new LinkedHashMap<>();

        map.put("artist", item.getArtist());
        map.put("song_id", item.getSongId());
        map.put("title", item.getTitle());
        map.put("year", item.getYear());
        map.put("album", item.getAlbum());
        map.put("image_url", item.getImageUrl());
        map.put("s3_image_key", item.getS3ImageKey());
        map.put("year_title_album", item.getYearTitleAlbum());
        map.put("artist_title_year", item.getArtistTitleYear());

        return map;
    }

    public static class UpdateUserRequest {
        @JsonProperty("user_name")
        private String userName;

        private String password;

        public UpdateUserRequest() {
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public static class CreateMusicRequest {
        private String title;
        private String artist;
        private String year;
        private String album;

        @JsonProperty("image_url")
        private String imageUrl;

        public CreateMusicRequest() {
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getArtist() {
            return artist;
        }

        public void setArtist(String artist) {
            this.artist = artist;
        }

        public String getYear() {
            return year;
        }

        public void setYear(String year) {
            this.year = year;
        }

        public String getAlbum() {
            return album;
        }

        public void setAlbum(String album) {
            this.album = album;
        }

        public String getImageUrl() {
            return imageUrl;
        }

        public void setImageUrl(String imageUrl) {
            this.imageUrl = imageUrl;
        }
    }

    public static class UpdateMusicRequest {
        private String title;
        private String year;
        private String album;

        @JsonProperty("image_url")
        private String imageUrl;

        public UpdateMusicRequest() {
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getYear() {
            return year;
        }

        public void setYear(String year) {
            this.year = year;
        }

        public String getAlbum() {
            return album;
        }

        public void setAlbum(String album) {
            this.album = album;
        }

        public String getImageUrl() {
            return imageUrl;
        }

        public void setImageUrl(String imageUrl) {
            this.imageUrl = imageUrl;
        }
    }
}