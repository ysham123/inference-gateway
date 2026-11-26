use std::{error::Error, fs, path::Path, time::Duration};

use serde_json::Value;

pub struct Profile {
    classes: Vec<(usize, u64, Duration)>,
    pub chat_start: Duration,
}

impl Profile {
    pub fn load(path: &Path) -> Result<Self, Box<dyn Error>> {
        let value: Value = serde_json::from_slice(&fs::read(path)?)?;
        let duration = |value: &Value| -> Result<Duration, Box<dyn Error>> {
            let ms = value
                .as_u64()
                .filter(|ms| *ms > 0)
                .ok_or("expected positive milliseconds")?;
            Ok(Duration::from_millis(ms))
        };
        let chat_start = duration(&value["chat_start_ms"])?;
        let rows = value["classes"]
            .as_array()
            .filter(|rows| !rows.is_empty())
            .ok_or("profile needs at least one class")?;
        let mut classes = Vec::with_capacity(rows.len());
        for row in rows {
            let bytes = row["prompt_bytes"]
                .as_u64()
                .filter(|bytes| *bytes > 0)
                .ok_or("invalid prompt_bytes")?;
            let tokens = row["max_tokens"]
                .as_u64()
                .filter(|tokens| *tokens > 0)
                .ok_or("invalid max_tokens")?;
            classes.push((
                usize::try_from(bytes)?,
                tokens,
                duration(&row["service_ms"])?,
            ));
        }
        Ok(Self {
            classes,
            chat_start,
        })
    }

    pub fn estimate(&self, body: &[u8]) -> Option<Duration> {
        let value: Value = serde_json::from_slice(body).ok()?;
        let messages = value["messages"].as_array()?;
        let [message] = messages.as_slice() else {
            return None;
        };
        if message["role"].as_str()? != "user" || value.get("tools").is_some() {
            return None;
        }
        let bytes = message["content"].as_str()?.len();
        let tokens = value
            .get("max_completion_tokens")
            .or_else(|| value.get("max_tokens"))?
            .as_u64()?;
        self.classes
            .iter()
            .filter(|(limit_bytes, limit_tokens, _)| {
                bytes <= *limit_bytes && tokens <= *limit_tokens
            })
            .min_by_key(|(bytes, tokens, _)| (*tokens, *bytes))
            .map(|(_, _, duration)| *duration)
    }
}
