// Frontend for the AWS Music Subscription App.
// For local testing, this calls the Spring Boot backend running on localhost:8080.
// For AWS deployment, change API_BASE_URL to your EC2/ECS/API Gateway base URL.

const API_BASE_URL = "https://hs1w63aqwj.execute-api.us-east-1.amazonaws.com/prod/api"

let currentSubscriptionSongIds = new Set();
let lastQuerySongs = [];

document.addEventListener("DOMContentLoaded", function () {
    setupLoginPage();
    setupRegisterPage();
    setupMainPage();
});

function setupLoginPage() {
    const loginForm = document.getElementById("loginForm");

    if (!loginForm) {
        return;
    }

    loginForm.addEventListener("submit", async function (event) {
        event.preventDefault();

        const email = document.getElementById("loginEmail").value.trim();
        const password = document.getElementById("loginPassword").value.trim();

        try {
            const response = await postJson(`${API_BASE_URL}/login`, {
                email: email,
                password: password
            });

            if (response.success) {
                sessionStorage.setItem("currentUser", JSON.stringify({
                    email: response.email,
                    user_name: response.user_name
                }));

                window.location.href = "main.html";
            } else {
                showMessage("loginMessage", "email or password is invalid", "error");
            }
        } catch (error) {
            showMessage("loginMessage", "Unable to connect to backend server", "error");
            console.error(error);
        }
    });
}

function setupRegisterPage() {
    const registerForm = document.getElementById("registerForm");

    if (!registerForm) {
        return;
    }

    registerForm.addEventListener("submit", async function (event) {
        event.preventDefault();

        const email = document.getElementById("registerEmail").value.trim();
        const userName = document.getElementById("registerUserName").value.trim();
        const password = document.getElementById("registerPassword").value.trim();

        try {
            const response = await postJson(`${API_BASE_URL}/register`, {
                email: email,
                user_name: userName,
                password: password
            });

            if (response.success) {
                showMessage("registerMessage", "Registration successful. Redirecting to login page...", "success");

                setTimeout(function () {
                    window.location.href = "login.html";
                }, 900);
            } else {
                showMessage("registerMessage", response.message || "The email already exists", "error");
            }
        } catch (error) {
            showMessage("registerMessage", "Unable to connect to backend server", "error");
            console.error(error);
        }
    });
}

function setupMainPage() {
    const queryForm = document.getElementById("queryForm");

    if (!queryForm) {
        return;
    }

    const currentUser = getCurrentUser();

    if (!currentUser) {
        window.location.href = "login.html";
        return;
    }

    document.getElementById("userArea").textContent = `Welcome, ${currentUser.user_name}`;

    const logoutButton = document.getElementById("logoutButton");

    logoutButton.addEventListener("click", function () {
        sessionStorage.removeItem("currentUser");
        window.location.href = "login.html";
    });

    loadSubscriptions();

    queryForm.addEventListener("submit", async function (event) {
        event.preventDefault();

        const title = document.getElementById("queryTitle").value.trim();
        const year = document.getElementById("queryYear").value.trim();
        const artist = document.getElementById("queryArtist").value.trim();
        const album = document.getElementById("queryAlbum").value.trim();

        if (!title && !year && !artist && !album) {
            showMessage("queryMessage", "At least one field must be completed", "error");
            clearContainer("queryResults");
            return;
        }

        const params = new URLSearchParams();

        if (title) {
            params.append("title", title);
        }

        if (year) {
            params.append("year", year);
        }

        if (artist) {
            params.append("artist", artist);
        }

        if (album) {
            params.append("album", album);
        }

        try {
            const response = await fetch(`${API_BASE_URL}/music/query?${params.toString()}`);
            const data = await response.json();

            if (!data.success || !data.songs || data.songs.length === 0) {
                showMessage("queryMessage", "No result is retrieved. Please query again", "error");
                clearContainer("queryResults");
                return;
            }

            showMessage("queryMessage", `Retrieved ${data.songs.length} result(s)`, "success");
            renderSongResults(data.songs);

        } catch (error) {
            showMessage("queryMessage", "Unable to query music data", "error");
            console.error(error);
        }
    });
}

async function loadSubscriptions() {
    const currentUser = getCurrentUser();

    if (!currentUser) {
        return;
    }

    try {
        const params = new URLSearchParams();
        params.append("email", currentUser.email);

        const response = await fetch(`${API_BASE_URL}/subscriptions?${params.toString()}`);
        const data = await response.json();

        const subscriptionList = document.getElementById("subscriptionList");
        clearContainer("subscriptionList");

        currentSubscriptionSongIds = new Set();

        if (data.subscriptions) {
            data.subscriptions.forEach(function (song) {
                currentSubscriptionSongIds.add(song.song_id);
            });
        }

        if (!data.subscriptions || data.subscriptions.length === 0) {
            showMessage("subscriptionMessage", "", "success");

            const emptyState = document.createElement("div");
            emptyState.className = "empty-state";
            emptyState.textContent = "No subscriptions yet. Query songs below and click Subscribe.";
            subscriptionList.appendChild(emptyState);

            return;
        }

        showMessage("subscriptionMessage", `You have ${data.subscriptions.length} subscribed song(s)`, "success");

        data.subscriptions.forEach(function (song) {
            const card = createSongCard(song, "remove");
            subscriptionList.appendChild(card);
        });

    } catch (error) {
        showMessage("subscriptionMessage", "Unable to load subscriptions", "error");
        console.error(error);
    }
}

function renderSongResults(songs) {
    lastQuerySongs = songs;

    const queryResults = document.getElementById("queryResults");
    clearContainer("queryResults");

    songs.forEach(function (song) {
        const card = createSongCard(song, "subscribe");
        queryResults.appendChild(card);
    });
}

function createSongCard(song, actionType) {
    const card = document.createElement("article");
    card.className = "song-card";

    const image = document.createElement("img");
    image.src = song.image_url || song.s3_image_url || "";
    image.alt = `${song.artist} artist image`;

    image.onerror = function () {
        image.style.display = "none";
    };

    const content = document.createElement("div");
    content.className = "song-card-content";

    const title = document.createElement("h3");
    title.className = "song-title";
    title.textContent = song.title || "Unknown title";

    const artist = document.createElement("p");
    artist.className = "song-meta";
    artist.textContent = `Artist: ${song.artist || "Unknown"}`;

    const year = document.createElement("p");
    year.className = "song-meta";
    year.textContent = `Year: ${song.year || "Unknown"}`;

    const album = document.createElement("p");
    album.className = "song-meta";
    album.textContent = `Album: ${song.album || "Unknown"}`;

    content.appendChild(title);
    content.appendChild(artist);
    content.appendChild(year);
    content.appendChild(album);

    const actions = document.createElement("div");
    actions.className = "song-card-actions";

    const button = document.createElement("button");

    if (actionType === "subscribe") {
        if (currentSubscriptionSongIds.has(song.song_id)) {
            button.textContent = "Subscribed";
            button.className = "subscribed-button";
            button.disabled = true;
        } else {
            button.textContent = "Subscribe";
            button.addEventListener("click", function () {
                subscribeToSong(song);
            });
        }
    } else {
        button.textContent = "Remove";
        button.className = "secondary-button";
        button.addEventListener("click", function () {
            removeSubscription(song);
        });
    }

    actions.appendChild(button);

    card.appendChild(image);
    card.appendChild(content);
    card.appendChild(actions);

    return card;
}

async function subscribeToSong(song) {
    const currentUser = getCurrentUser();

    if (!currentUser) {
        window.location.href = "login.html";
        return;
    }

    try {
        const response = await postJson(`${API_BASE_URL}/subscriptions`, {
            email: currentUser.email,
            artist: song.artist,
            song_id: song.song_id
        });

        if (response.success) {
            showMessage("queryMessage", "Song subscribed successfully", "success");

            await loadSubscriptions();
            renderSongResults(lastQuerySongs);

        } else {
            showMessage("queryMessage", response.message || "Song is already subscribed", "error");

            await loadSubscriptions();
            renderSongResults(lastQuerySongs);
        }

    } catch (error) {
        showMessage("queryMessage", "Unable to subscribe to song", "error");
        console.error(error);
    }
}

async function removeSubscription(song) {
    const currentUser = getCurrentUser();

    if (!currentUser) {
        window.location.href = "login.html";
        return;
    }

    try {
        const params = new URLSearchParams();
        params.append("email", currentUser.email);
        params.append("song_id", song.song_id);

        const response = await fetch(`${API_BASE_URL}/subscriptions?${params.toString()}`, {
            method: "DELETE"
        });

        const data = await response.json();

        if (data.success) {
            showMessage("subscriptionMessage", "Subscription removed successfully", "success");

            await loadSubscriptions();

            if (lastQuerySongs.length > 0) {
                renderSongResults(lastQuerySongs);
            }

        } else {
            showMessage("subscriptionMessage", data.message || "Subscription not found", "error");
        }

    } catch (error) {
        showMessage("subscriptionMessage", "Unable to remove subscription", "error");
        console.error(error);
    }
}

async function postJson(url, payload) {
    const response = await fetch(url, {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify(payload)
    });

    return response.json();
}

function getCurrentUser() {
    const userJson = sessionStorage.getItem("currentUser");

    if (!userJson) {
        return null;
    }

    try {
        return JSON.parse(userJson);
    } catch (error) {
        sessionStorage.removeItem("currentUser");
        return null;
    }
}

function showMessage(elementId, message, type) {
    const element = document.getElementById(elementId);

    if (!element) {
        return;
    }

    element.textContent = message;
    element.className = `message ${type}`;
}

function clearContainer(elementId) {
    const element = document.getElementById(elementId);

    if (!element) {
        return;
    }

    element.innerHTML = "";
}