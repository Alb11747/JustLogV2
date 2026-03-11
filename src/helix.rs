use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::config::Config;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserData {
    pub id: String,
    pub login: String,
    #[serde(rename = "display_name")]
    pub display_name: String,
    #[serde(default)]
    pub r#type: String,
    #[serde(default, rename = "broadcaster_type")]
    pub broadcaster_type: String,
    #[serde(default)]
    pub description: String,
    #[serde(default, rename = "profile_image_url")]
    pub profile_image_url: String,
    #[serde(default, rename = "offline_image_url")]
    pub offline_image_url: String,
    #[serde(default, rename = "view_count")]
    pub view_count: i64,
    #[serde(default)]
    pub email: String,
}

#[derive(Clone)]
pub struct HelixClient {
    client: reqwest::Client,
    base_url: String,
    token_url: String,
    client_id: String,
    client_secret: String,
    access_token: Arc<RwLock<Option<String>>>,
    cache_by_id: Arc<Mutex<HashMap<String, UserData>>>,
    cache_by_login: Arc<Mutex<HashMap<String, UserData>>>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize)]
struct UsersResponse {
    data: Vec<UserData>,
}

impl HelixClient {
    pub fn new(config: &Config) -> Self {
        let base_url = if config.helix.base_url.is_empty() {
            "https://api.twitch.tv/helix".to_string()
        } else {
            config.helix.base_url.trim_end_matches('/').to_string()
        };
        let token_url = if base_url.contains("/helix") {
            base_url.replacen("/helix", "/oauth2/token", 1)
        } else {
            format!("{base_url}/oauth2/token")
        };
        Self {
            client: reqwest::Client::new(),
            base_url,
            token_url,
            client_id: config.client_id.clone(),
            client_secret: config.client_secret.clone(),
            access_token: Arc::new(RwLock::new(None)),
            cache_by_id: Arc::new(Mutex::new(HashMap::new())),
            cache_by_login: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_users_by_ids(&self, ids: &[String]) -> Result<HashMap<String, UserData>> {
        let mut missing = Vec::new();
        {
            let cache = self.cache_by_id.lock().await;
            for id in ids {
                if !cache.contains_key(id) {
                    missing.push(id.clone());
                }
            }
        }
        if !missing.is_empty() {
            self.fetch_users("id", &missing).await?;
        }
        let cache = self.cache_by_id.lock().await;
        Ok(ids
            .iter()
            .filter_map(|id| cache.get(id).cloned().map(|user| (id.clone(), user)))
            .collect())
    }

    pub async fn get_users_by_logins(&self, logins: &[String]) -> Result<HashMap<String, UserData>> {
        let normalized = logins.iter().map(|login| login.to_lowercase()).collect::<Vec<_>>();
        let mut missing = Vec::new();
        {
            let cache = self.cache_by_login.lock().await;
            for login in &normalized {
                if !cache.contains_key(login) {
                    missing.push(login.clone());
                }
            }
        }
        if !missing.is_empty() {
            self.fetch_users("login", &missing).await?;
        }
        let cache = self.cache_by_login.lock().await;
        Ok(normalized
            .iter()
            .filter_map(|login| cache.get(login).cloned().map(|user| (login.clone(), user)))
            .collect())
    }

    async fn fetch_users(&self, query_key: &str, values: &[String]) -> Result<()> {
        let token = self.ensure_token().await?;
        for chunk in values.chunks(100) {
            let mut request = self
                .client
                .get(format!("{}/users", self.base_url))
                .header("Client-Id", &self.client_id)
                .bearer_auth(&token);
            for value in chunk {
                request = request.query(&[(query_key, value)]);
            }
            let response = request.send().await?.error_for_status()?;
            let users = response.json::<UsersResponse>().await?;
            let mut cache_by_id = self.cache_by_id.lock().await;
            let mut cache_by_login = self.cache_by_login.lock().await;
            for user in users.data {
                cache_by_login.insert(user.login.to_lowercase(), user.clone());
                cache_by_id.insert(user.id.clone(), user);
            }
        }
        Ok(())
    }

    async fn ensure_token(&self) -> Result<String> {
        if let Some(token) = self.access_token.read().await.clone() {
            return Ok(token);
        }
        let response = self
            .client
            .post(&self.token_url)
            .query(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("grant_type", "client_credentials"),
            ])
            .send()
            .await
            .with_context(|| format!("failed to request token from {}", self.token_url))?;
        let response = response.error_for_status().map_err(|error| anyhow!(error))?;
        let token = response.json::<TokenResponse>().await?.access_token;
        *self.access_token.write().await = Some(token.clone());
        Ok(token)
    }
}
