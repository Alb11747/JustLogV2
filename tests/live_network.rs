use std::env;

use reqwest::Client;

#[tokio::test]
async fn outbound_https_smoke_check_is_opt_in() {
    if env::var("JUSTLOG_RUN_LIVE_NETWORK_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping live network smoke test; set JUSTLOG_RUN_LIVE_NETWORK_TESTS=1 to enable"
        );
        return;
    }

    let response = Client::new()
        .get("https://example.com/")
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
}

#[tokio::test]
async fn optional_twitch_helix_smoke_check_uses_explicit_credentials() {
    if env::var("JUSTLOG_RUN_LIVE_NETWORK_TESTS").as_deref() != Ok("1") {
        eprintln!(
            "skipping Twitch Helix smoke test; set JUSTLOG_RUN_LIVE_NETWORK_TESTS=1 to enable"
        );
        return;
    }

    let client_id = match env::var("TWITCH_CLIENT_ID") {
        Ok(value) if !value.is_empty() => value,
        _ => {
            eprintln!("skipping Twitch Helix smoke test; TWITCH_CLIENT_ID is not set");
            return;
        }
    };
    let client_secret = match env::var("TWITCH_CLIENT_SECRET") {
        Ok(value) if !value.is_empty() => value,
        _ => {
            eprintln!("skipping Twitch Helix smoke test; TWITCH_CLIENT_SECRET is not set");
            return;
        }
    };

    let client = Client::new();
    let token = client
        .post("https://id.twitch.tv/oauth2/token")
        .query(&[
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("grant_type", "client_credentials"),
        ])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let token_json = token.json::<serde_json::Value>().await.unwrap();
    let access_token = token_json["access_token"].as_str().unwrap().to_string();

    let users = client
        .get("https://api.twitch.tv/helix/users")
        .header("Client-Id", client_id)
        .bearer_auth(access_token)
        .query(&[("login", "twitch")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let body = users.text().await.unwrap();
    assert!(body.contains("\"login\":\"twitch\""));
}
