use crate::{
    error::{AppError, Result},
    services::auth::User,
    state::AppState,
};
use axum::{
    extract::{State, Extension},
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{info, error};

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/config", get(get_ai_config).post(save_ai_config))
        .route("/generate", post(generate_content))
        .route("/improve", post(improve_content))
        .route("/suggest-title", post(suggest_title))
        .route("/suggest-tags", post(suggest_tags))
        .route("/models", get(list_models))
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AiConfig {
    pub provider: String,        // "anthropic" | "openai" | "custom"
    pub api_key: String,
    pub model: String,
    pub base_url: Option<String>,
    pub enabled: bool,
    pub auto_save: bool,
    pub auto_suggest_tags: bool,
    pub auto_suggest_title: bool,
}

#[derive(Debug, Deserialize)]
pub struct GenerateRequest {
    pub prompt: String,
    pub context: Option<String>,  // existing article content as context
    pub style: Option<String>,    // "technical" | "casual" | "formal"
    pub max_tokens: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ImproveRequest {
    pub content: String,
    pub instruction: Option<String>,  // e.g. "make it more concise", "fix grammar"
}

#[derive(Debug, Deserialize)]
pub struct SuggestTitleRequest {
    pub content: String,
}

#[derive(Debug, Deserialize)]
pub struct SuggestTagsRequest {
    pub title: String,
    pub content: String,
}

/// GET /api/blog/ai/config - Get user's AI configuration
pub async fn get_ai_config(
    State(app_state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
) -> Result<Json<Value>> {
    let query = "SELECT * FROM ai_config WHERE user_id = $user_id LIMIT 1";
    let params = json!({ "user_id": user.id });

    let mut response = app_state.db.query_with_params(query, params).await?;
    let records: Vec<serde_json::Value> = response.take(0).unwrap_or_default();

    let raw = match records.into_iter().next() {
        Some(v) => v,
        None => {
            return Ok(Json(json!({
                "success": true,
                "data": null
            })));
        }
    };

    // Build clean response — mask api_key, normalize base_url
    let api_key = raw.get("api_key").and_then(|v| v.as_str()).unwrap_or("");
    let api_key_masked = if api_key.len() > 8 {
        format!("{}...{}", &api_key[..4], &api_key[api_key.len()-4..])
    } else if !api_key.is_empty() {
        "****".to_string()
    } else {
        String::new()
    };

    let base_url = raw.get("base_url")
        .and_then(|v| if v.is_null() || v.as_str() == Some("Null") { None } else { v.as_str() })
        .map(|s| json!(s))
        .unwrap_or(Value::Null);

    let config = json!({
        "provider": raw.get("provider").and_then(|v| v.as_str()).unwrap_or("anthropic"),
        "api_key": "",                          // never send real key
        "api_key_masked": api_key_masked,
        "has_api_key": !api_key.is_empty(),
        "model": raw.get("model").and_then(|v| v.as_str()).unwrap_or("claude-sonnet-4-6"),
        "base_url": base_url,
        "enabled": raw.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true),
        "auto_save": raw.get("auto_save").and_then(|v| v.as_bool()).unwrap_or(false),
        "auto_suggest_tags": raw.get("auto_suggest_tags").and_then(|v| v.as_bool()).unwrap_or(false),
        "auto_suggest_title": raw.get("auto_suggest_title").and_then(|v| v.as_bool()).unwrap_or(false),
        "updated_at": raw.get("updated_at"),
    });

    Ok(Json(json!({
        "success": true,
        "data": config
    })))
}

/// POST /api/blog/ai/config - Save user's AI configuration
pub async fn save_ai_config(
    State(app_state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
    Json(config): Json<AiConfig>,
) -> Result<Json<Value>> {
    // Validate provider
    if !["anthropic", "openai", "custom"].contains(&config.provider.as_str()) {
        return Err(AppError::BadRequest("Invalid provider. Use: anthropic, openai, or custom".to_string()));
    }

    // If api_key is empty, keep the existing one
    let api_key = if config.api_key.is_empty() {
        let query = "SELECT api_key FROM ai_config WHERE user_id = $user_id LIMIT 1";
        let params = json!({ "user_id": user.id });
        let mut resp = app_state.db.query_with_params(query, params).await?;
        let records: Vec<Value> = resp.take(0).unwrap_or_default();
        records.into_iter().next()
            .and_then(|v| v.get("api_key").and_then(|k| k.as_str()).map(|s| s.to_string()))
            .unwrap_or_default()
    } else {
        config.api_key.clone()
    };

    // Upsert config
    let query = r#"
        BEGIN TRANSACTION;
        DELETE ai_config WHERE user_id = $user_id;
        CREATE ai_config SET
            user_id = $user_id,
            provider = $provider,
            api_key = $api_key,
            model = $model,
            base_url = $base_url,
            enabled = $enabled,
            auto_save = $auto_save,
            auto_suggest_tags = $auto_suggest_tags,
            auto_suggest_title = $auto_suggest_title,
            updated_at = time::now();
        COMMIT TRANSACTION;
    "#;

    let params = json!({
        "user_id": user.id,
        "provider": config.provider,
        "api_key": api_key,
        "model": config.model,
        "base_url": config.base_url,
        "enabled": config.enabled,
        "auto_save": config.auto_save,
        "auto_suggest_tags": config.auto_suggest_tags,
        "auto_suggest_title": config.auto_suggest_title,
    });

    app_state.db.query_with_params(query, params).await?;

    info!("AI config saved for user {}", user.id);

    Ok(Json(json!({
        "success": true,
        "message": "AI configuration saved successfully"
    })))
}

/// POST /api/blog/ai/generate - Generate content using AI
pub async fn generate_content(
    State(app_state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
    Json(req): Json<GenerateRequest>,
) -> Result<Json<Value>> {
    let config = get_user_ai_config(&app_state, &user.id).await?;

    let system_prompt = build_system_prompt(req.style.as_deref());
    let user_message = if let Some(ctx) = &req.context {
        format!("Context (existing article):\n{}\n\nTask: {}", ctx, req.prompt)
    } else {
        req.prompt.clone()
    };

    let result = call_ai_api(&config, &system_prompt, &user_message, req.max_tokens.unwrap_or(2000)).await?;

    Ok(Json(json!({
        "success": true,
        "data": {
            "content": result,
            "model": config.model,
            "provider": config.provider
        }
    })))
}

/// POST /api/blog/ai/improve - Improve existing content
pub async fn improve_content(
    State(app_state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
    Json(req): Json<ImproveRequest>,
) -> Result<Json<Value>> {
    let config = get_user_ai_config(&app_state, &user.id).await?;

    let instruction = req.instruction.as_deref().unwrap_or("Improve the quality, clarity and flow of this content");
    let system_prompt = "You are a professional blog editor. Improve the given content while preserving the author's voice and key ideas. Return only the improved content in the same format (Markdown).";
    let user_message = format!("Instruction: {}\n\nContent to improve:\n{}", instruction, req.content);

    let result = call_ai_api(&config, system_prompt, &user_message, 3000).await?;

    Ok(Json(json!({
        "success": true,
        "data": {
            "content": result
        }
    })))
}

/// POST /api/blog/ai/suggest-title - Suggest titles for content
pub async fn suggest_title(
    State(app_state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
    Json(req): Json<SuggestTitleRequest>,
) -> Result<Json<Value>> {
    let config = get_user_ai_config(&app_state, &user.id).await?;

    let system_prompt = "You are a blog title expert. Generate 5 compelling, SEO-friendly title suggestions for the given content. Return ONLY a JSON array of strings, no other text. Example: [\"Title 1\", \"Title 2\", \"Title 3\", \"Title 4\", \"Title 5\"]";
    let content_preview = if req.content.len() > 500 { &req.content[..500] } else { &req.content };
    let user_message = format!("Generate titles for this blog post:\n{}", content_preview);

    let result = call_ai_api(&config, system_prompt, &user_message, 300).await?;

    // Parse JSON array from result
    let titles: Vec<String> = serde_json::from_str(&result)
        .or_else(|_| {
            // Try to extract JSON array from the response
            let start = result.find('[').unwrap_or(0);
            let end = result.rfind(']').map(|i| i + 1).unwrap_or(result.len());
            serde_json::from_str(&result[start..end])
        })
        .unwrap_or_else(|_| {
            result.lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| l.trim_start_matches(|c: char| c.is_ascii_digit() || c == '.' || c == ' ').to_string())
                .collect()
        });

    Ok(Json(json!({
        "success": true,
        "data": {
            "titles": titles
        }
    })))
}

/// POST /api/blog/ai/suggest-tags - Suggest tags
pub async fn suggest_tags(
    State(app_state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
    Json(req): Json<SuggestTagsRequest>,
) -> Result<Json<Value>> {
    let config = get_user_ai_config(&app_state, &user.id).await?;

    let system_prompt = "You are a content tagging expert. Suggest 5-8 relevant tags for the given blog post. Return ONLY a JSON array of lowercase strings, no spaces in tags (use hyphens). Example: [\"rust\", \"web-development\", \"backend\"]";
    let content_preview = if req.content.len() > 400 { &req.content[..400] } else { &req.content };
    let user_message = format!("Title: {}\n\nContent:\n{}", req.title, content_preview);

    let result = call_ai_api(&config, system_prompt, &user_message, 200).await?;

    let tags: Vec<String> = serde_json::from_str(&result)
        .or_else(|_| {
            let start = result.find('[').unwrap_or(0);
            let end = result.rfind(']').map(|i| i + 1).unwrap_or(result.len());
            serde_json::from_str(&result[start..end])
        })
        .unwrap_or_else(|_| {
            result.split(',')
                .map(|t| t.trim().trim_matches('"').to_lowercase())
                .filter(|t| !t.is_empty())
                .collect()
        });

    Ok(Json(json!({
        "success": true,
        "data": {
            "tags": tags
        }
    })))
}

/// GET /api/blog/ai/models - List available models for each provider
pub async fn list_models(
    Extension(_user): Extension<User>,
) -> Result<Json<Value>> {
    Ok(Json(json!({
        "success": true,
        "data": {
            "anthropic": [
                {"id": "claude-sonnet-4-6", "name": "Claude Sonnet 4.6 (推荐)", "description": "最新最强，适合复杂写作任务"},
                {"id": "claude-haiku-4-5-20251001", "name": "Claude Haiku 4.5", "description": "速度快，适合简单任务"},
                {"id": "claude-opus-4-7", "name": "Claude Opus 4.7", "description": "最强能力，适合深度创作"}
            ],
            "openai": [
                {"id": "gpt-4o", "name": "GPT-4o (推荐)", "description": "最新多模态模型"},
                {"id": "gpt-4o-mini", "name": "GPT-4o Mini", "description": "高性价比"},
                {"id": "gpt-4-turbo", "name": "GPT-4 Turbo", "description": "强力推理"}
            ],
            "custom": [
                {"id": "custom", "name": "自定义模型", "description": "兼容 OpenAI API 格式的任意模型"}
            ]
        }
    })))
}

// ── Helpers ──

async fn get_user_ai_config(app_state: &AppState, user_id: &str) -> Result<AiConfig> {
    let query = "SELECT * FROM ai_config WHERE user_id = $user_id LIMIT 1";
    let params = json!({ "user_id": user_id });

    let mut response = app_state.db.query_with_params(query, params).await?;
    let raw: Option<Value> = response.take(0).unwrap_or(None);

    match raw {
        Some(v) if !v.is_null() => {
            let provider = v.get("provider").and_then(|x| x.as_str()).unwrap_or("anthropic").to_string();
            let api_key = v.get("api_key").and_then(|x| x.as_str()).unwrap_or("").to_string();
            if api_key.is_empty() {
                return Err(AppError::BadRequest("AI API key not configured. Please configure it in AI Settings.".to_string()));
            }
            Ok(AiConfig {
                provider,
                api_key,
                model: v.get("model").and_then(|x| x.as_str()).unwrap_or("claude-sonnet-4-6").to_string(),
                base_url: v.get("base_url").and_then(|x| x.as_str()).map(|s| s.to_string()),
                enabled: v.get("enabled").and_then(|x| x.as_bool()).unwrap_or(true),
                auto_save: v.get("auto_save").and_then(|x| x.as_bool()).unwrap_or(false),
                auto_suggest_tags: v.get("auto_suggest_tags").and_then(|x| x.as_bool()).unwrap_or(false),
                auto_suggest_title: v.get("auto_suggest_title").and_then(|x| x.as_bool()).unwrap_or(false),
            })
        }
        _ => Err(AppError::BadRequest("AI not configured. Please add your API key in AI Settings.".to_string())),
    }
}

fn build_system_prompt(style: Option<&str>) -> String {
    let style_instruction = match style {
        Some("technical") => "Write in a technical, precise style suitable for developers and technical readers.",
        Some("casual") => "Write in a casual, conversational style that's friendly and approachable.",
        Some("formal") => "Write in a formal, professional style.",
        _ => "Write in a clear, engaging style suitable for a general tech blog audience.",
    };
    format!(
        "You are an expert blog writer specializing in technology topics. {}
You write in Markdown format. Use headers, code blocks, and bullet points where appropriate.
Focus on clarity, accuracy, and reader engagement. Do not include meta-commentary about your response.",
        style_instruction
    )
}

async fn call_ai_api(config: &AiConfig, system: &str, user_msg: &str, max_tokens: u32) -> Result<String> {
    if !config.enabled {
        return Err(AppError::BadRequest("AI is disabled. Enable it in AI Settings.".to_string()));
    }

    match config.provider.as_str() {
        "anthropic" => call_anthropic(config, system, user_msg, max_tokens).await,
        "openai" | "custom" => call_openai_compatible(config, system, user_msg, max_tokens).await,
        _ => Err(AppError::BadRequest(format!("Unknown provider: {}", config.provider))),
    }
}

async fn call_anthropic(config: &AiConfig, system: &str, user_msg: &str, max_tokens: u32) -> Result<String> {
    let client = reqwest::Client::new();
    let url = "https://api.anthropic.com/v1/messages";

    let body = json!({
        "model": config.model,
        "max_tokens": max_tokens,
        "system": system,
        "messages": [{"role": "user", "content": user_msg}]
    });

    let response = client
        .post(url)
        .header("x-api-key", &config.api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| AppError::Internal(format!("Anthropic API request failed: {}", e)))?;

    let status = response.status();
    let response_json: Value = response.json().await
        .map_err(|e| AppError::Internal(format!("Failed to parse Anthropic response: {}", e)))?;

    if !status.is_success() {
        let err_msg = response_json.get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or("Unknown Anthropic API error");
        return Err(AppError::Internal(format!("Anthropic API error: {}", err_msg)));
    }

    let text = response_json
        .get("content")
        .and_then(|c| c.as_array())
        .and_then(|arr| arr.first())
        .and_then(|item| item.get("text"))
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();

    Ok(text)
}

async fn call_openai_compatible(config: &AiConfig, system: &str, user_msg: &str, max_tokens: u32) -> Result<String> {
    let client = reqwest::Client::new();
    let base_url = config.base_url.as_deref().unwrap_or("https://api.openai.com");
    let url = format!("{}/v1/chat/completions", base_url.trim_end_matches('/'));

    let model = if config.provider == "custom" && config.model == "custom" {
        "gpt-4o".to_string()
    } else {
        config.model.clone()
    };

    let body = json!({
        "model": model,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg}
        ]
    });

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", config.api_key))
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| AppError::Internal(format!("OpenAI API request failed: {}", e)))?;

    let status = response.status();
    let response_json: Value = response.json().await
        .map_err(|e| AppError::Internal(format!("Failed to parse OpenAI response: {}", e)))?;

    if !status.is_success() {
        let err_msg = response_json.get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or("Unknown OpenAI API error");
        return Err(AppError::Internal(format!("OpenAI API error: {}", err_msg)));
    }

    let text = response_json
        .get("choices")
        .and_then(|c| c.as_array())
        .and_then(|arr| arr.first())
        .and_then(|item| item.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();

    Ok(text)
}
