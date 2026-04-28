use crate::{
    error::{AppError, Result},
    services::auth::User,
    utils::middleware::OptionalAuth,
    state::AppState,
};
use axum::{
    extract::State,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tracing::{info, debug, warn};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use argon2::password_hash::{SaltString, rand_core::OsRng};
use jsonwebtoken::{encode, EncodingKey, Header};
use crate::services::auth::Claims;
use chrono::{Utc, Duration};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub email: String,
    pub password: String,
    pub username: String,
    pub display_name: Option<String>,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/login", post(login))
        .route("/register", post(register))
        .route("/logout", post(logout))
        .route("/me", get(get_current_user))
        .route("/status", get(get_auth_status))
        .route("/refresh", get(get_auth_info))
        .route("/email-status", get(get_email_verification_status))
}

fn create_jwt(user_id: &str, email: &str, jwt_secret: &str) -> Result<String> {
    let now = Utc::now();
    let exp = (now + Duration::days(7)).timestamp();
    let claims = Claims {
        sub: user_id.to_string(),
        exp,
        iat: now.timestamp(),
        session_id: Some(Uuid::new_v4().to_string()),
        email: Some(email.to_string()),
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(jwt_secret.as_ref()),
    )
    .map_err(|e| AppError::Internal(format!("Failed to create JWT: {}", e)))
}

pub async fn register(
    State(app_state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<Value>> {
    // Hash password
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let password_hash = argon2
        .hash_password(req.password.as_bytes(), &salt)
        .map_err(|e| AppError::Internal(format!("Failed to hash password: {}", e)))?
        .to_string();

    let user_id = Uuid::new_v4().to_string();
    let display_name = req.display_name.clone().unwrap_or_else(|| req.username.clone());

    // Check if username or email already taken
    let existing = app_state.db.query_with_params(
        "SELECT id FROM user_profile WHERE username = $username OR user_id IN (SELECT id FROM user_auth WHERE email = $email)",
        serde_json::json!({ "username": req.username, "email": req.email }),
    ).await;

    // Create user_auth record with credentials (user_id as explicit field)
    app_state.db.query_with_params(
        "CREATE user_auth SET user_id = $uid, email = $email, password_hash = $hash, created_at = time::now()",
        serde_json::json!({
            "uid": user_id,
            "email": req.email,
            "hash": password_hash,
        }),
    ).await.map_err(|e| AppError::Database(surrealdb::Error::thrown(e.to_string())))?;

    // Create user profile
    app_state.user_service.get_or_create_profile(
        &user_id,
        &req.email,
        false,
        Some(req.username.clone()),
        Some(display_name.clone()),
    ).await.map_err(|e| {
        warn!("Profile creation failed: {}", e);
        AppError::Internal("Failed to create user profile".to_string())
    })?;

    let token = create_jwt(&user_id, &req.email, &app_state.config.jwt_secret)?;

    info!("User registered: {} ({})", req.username, req.email);

    Ok(Json(json!({
        "success": true,
        "token": token,
        "user": {
            "id": user_id,
            "email": req.email,
            "username": req.username,
            "display_name": display_name,
            "is_verified": false,
        }
    })))
}

pub async fn login(
    State(app_state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<Value>> {
    // Look up user_auth by email
    let mut resp = app_state.db.query_with_params(
        "SELECT user_id, email, password_hash FROM user_auth WHERE email = $email LIMIT 1",
        serde_json::json!({ "email": req.email }),
    ).await.map_err(|e| AppError::Database(surrealdb::Error::thrown(e.to_string())))?;

    let records: Vec<Value> = resp.take(0).map_err(|e| AppError::Internal(e.to_string()))?;
    let record = records.into_iter().next()
        .ok_or_else(|| AppError::Authentication("Invalid email or password".to_string()))?;

    let stored_hash = record.get("password_hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::Internal("Invalid credential record".to_string()))?;

    let user_id = record.get("user_id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Verify password
    let parsed_hash = PasswordHash::new(stored_hash)
        .map_err(|e| AppError::Internal(format!("Invalid stored hash: {}", e)))?;
    Argon2::default()
        .verify_password(req.password.as_bytes(), &parsed_hash)
        .map_err(|_| AppError::Authentication("Invalid email or password".to_string()))?;

    // Load profile
    let profile = app_state.user_service.get_or_create_profile(
        &user_id, &req.email, false, None, None,
    ).await;

    let (username, display_name) = match &profile {
        Ok(p) => (p.username.clone(), p.display_name.clone()),
        Err(_) => (req.email.split('@').next().unwrap_or("user").to_string(), req.email.clone()),
    };

    let token = create_jwt(&user_id, &req.email, &app_state.config.jwt_secret)?;

    info!("User logged in: {} ({})", username, req.email);

    Ok(Json(json!({
        "success": true,
        "token": token,
        "user": {
            "id": user_id,
            "email": req.email,
            "username": username,
            "display_name": display_name,
            "is_verified": false,
        }
    })))
}

pub async fn logout(
    OptionalAuth(_user): OptionalAuth,
) -> Result<Json<Value>> {
    Ok(Json(json!({ "success": true, "message": "Logged out" })))
}

/// 获取当前用户信息
/// GET /api/auth/me
/// 
/// 注意：实际的用户认证是由 Rainbow-Gateway 和 Rainbow-Auth 处理的
/// 这个端点主要是返回通过 JWT 解析得到的用户信息
pub async fn get_current_user(
    State(app_state): State<Arc<AppState>>,
    OptionalAuth(user): OptionalAuth,
) -> Result<Json<Value>> {
    let user = match user {
        Some(user) => user,
        None => return Err(AppError::Authentication("Not authenticated".to_string())),
    };
    debug!("Getting current user info for user: {}", user.id);

    // 获取或创建用户资料（包含邮箱验证状态）
    let profile = app_state.user_service.get_or_create_profile(
        &user.id,
        &user.email,
        user.is_verified, // Rainbow-Auth的邮箱验证状态
        user.username.clone(),
        user.display_name.clone(),
    ).await;

    // 获取用户活动统计
    let stats = app_state.user_service.get_user_stats(&user.id).await;

    let profile_json = match profile {
        Ok(p) => json!(p.to_response()),
        Err(e) => {
            warn!("get_or_create_profile failed in /auth/me for user {}: {}", user.id, e);
            Value::Null
        }
    };
    let stats_json = match stats {
        Ok(s) => json!(s),
        Err(e) => {
            warn!("get_user_stats failed in /auth/me for user {}: {}", user.id, e);
            json!({
                "articles_written": 0,
                "comments_made": 0,
                "claps_given": 0,
                "claps_received": 0,
                "followers": 0,
                "following": 0
            })
        }
    };

    Ok(Json(json!({
        "success": true,
        "data": {
            "authenticated": true,
            "user": {
                "id": user.id,
                "email": user.email,
                "username": user.username,
                "display_name": user.display_name,
                "avatar_url": user.avatar_url,
                "is_verified": user.is_verified,
                "created_at": user.created_at,
                "roles": user.roles,
                "permissions": user.permissions,
            },
            "auth": {
                "id": user.id,
                "email": user.email,
                "username": user.username,
                "display_name": user.display_name,
                "avatar_url": user.avatar_url,
                "is_verified": user.is_verified,
                "created_at": user.created_at,
                "roles": user.roles,
                "permissions": user.permissions,
            },
            "profile": profile_json,
            "activity": stats_json
        }
    })))
}

/// 获取认证状态
/// GET /api/auth/status
/// 
/// 这个端点可以被未认证的用户访问，用于检查当前的认证状态
pub async fn get_auth_status(
    State(_app_state): State<Arc<AppState>>,
    OptionalAuth(user): OptionalAuth,
) -> Result<Json<Value>> {
    debug!("Checking authentication status");

    match user {
        Some(user) => {
            Ok(Json(json!({
                "success": true,
                "data": {
                    "authenticated": true,
                    "user": {
                        "id": user.id,
                        "email": user.email,
                        "username": user.username,
                        "display_name": user.display_name,
                        "avatar_url": user.avatar_url,
                        "is_verified": user.is_verified,
                        "roles": user.roles,
                    }
                }
            })))
        }
        None => {
            Ok(Json(json!({
                "success": true,
                "data": {
                    "authenticated": false,
                    "user": null,
                    "message": "Not authenticated. Please login through Rainbow-Gateway."
                }
            })))
        }
    }
}

/// 获取认证信息和配置
/// GET /api/auth/refresh
/// 
/// 用于刷新用户的认证状态和获取最新的用户信息
pub async fn get_auth_info(
    State(app_state): State<Arc<AppState>>,
    OptionalAuth(user): OptionalAuth,
) -> Result<Json<Value>> {
    let user = match user {
        Some(user) => user,
        None => return Err(AppError::Authentication("Not authenticated".to_string())),
    };
    debug!("Refreshing auth info for user: {}", user.id);

    // 获取最新的用户资料
    let profile = app_state.user_service.get_or_create_profile(
        &user.id,
        &user.email,
        user.is_verified,
        user.username.clone(),
        user.display_name.clone(),
    ).await;

    // 获取用户活动统计
    let stats = app_state.user_service.get_user_stats(&user.id).await;

    let profile_json = match profile {
        Ok(p) => json!(p.to_response()),
        Err(e) => {
            warn!("get_or_create_profile failed in /auth/refresh for user {}: {}", user.id, e);
            Value::Null
        }
    };
    let stats_json = match stats {
        Ok(s) => json!(s),
        Err(e) => {
            warn!("get_user_stats failed in /auth/refresh for user {}: {}", user.id, e);
            json!({
                "articles_written": 0,
                "comments_made": 0,
                "claps_given": 0,
                "claps_received": 0,
                "followers": 0,
                "following": 0
            })
        }
    };

    // 获取系统配置（用户相关的）
    let user_config = json!({
        "features": {
            "can_create_articles": app_state.auth_service.check_permission(&user.id, "article.create").await.unwrap_or(false),
            "can_create_publications": app_state.auth_service.check_permission(&user.id, "publication.create").await.unwrap_or(false),
            "can_comment": app_state.auth_service.check_permission(&user.id, "comment.create").await.unwrap_or(false),
        },
        "limits": {
            "max_article_length": app_state.config.max_article_length,
            "max_comment_length": app_state.config.max_comment_length,
            "max_bio_length": app_state.config.max_bio_length,
        }
    });

    info!("Refreshed auth info for user: {}", user.id);

    Ok(Json(json!({
        "success": true,
        "data": {
            "authenticated": true,
            "user": {
                "id": user.id,
                "email": user.email,
                "username": user.username,
                "display_name": user.display_name,
                "avatar_url": user.avatar_url,
                "is_verified": user.is_verified,
                "created_at": user.created_at,
                "roles": user.roles,
                "permissions": user.permissions,
            },
            "auth": {
                "id": user.id,
                "email": user.email,
                "username": user.username,
                "display_name": user.display_name,
                "avatar_url": user.avatar_url,
                "is_verified": user.is_verified,
                "created_at": user.created_at,
                "roles": user.roles,
                "permissions": user.permissions,
            },
            "profile": profile_json,
            "activity": stats_json,
            "config": user_config
        },
        "message": "Authentication info refreshed successfully"
    })))
}

/// 获取邮箱验证状态
/// GET /api/auth/email-status
/// 
/// 专门用于检查用户邮箱验证状态的端点
pub async fn get_email_verification_status(
    State(app_state): State<Arc<AppState>>,
    OptionalAuth(user): OptionalAuth,
) -> Result<Json<Value>> {
    let user = match user {
        Some(user) => user,
        None => return Err(AppError::Authentication("Not authenticated".to_string())),
    };
    debug!("Getting email verification status for user: {}", user.id);

    // 获取用户资料（包含最新的邮箱验证状态）
    let profile = app_state.user_service.get_or_create_profile(
        &user.id,
        &user.email,
        user.is_verified,
        user.username.clone(),
        user.display_name.clone(),
    ).await;

    if let Err(e) = &profile {
        warn!("get_or_create_profile failed in /auth/email-status for user {}: {}", user.id, e);
    }

    Ok(Json(json!({
        "success": true,
        "data": {
            "user_id": user.id,
            "email": user.email,
            "email_verified": user.is_verified,
            "verification_required_for": {
                "creating_articles": !user.is_verified,
                "commenting": !user.is_verified,
                "following_users": false,
                "publishing_articles": !user.is_verified
            },
            "rainbow_auth_url": format!("{}/api/auth", app_state.config.auth_service_url),
            "verification_help": {
                "message": if user.is_verified {
                    "您的邮箱已经通过验证"
                } else {
                    "您的邮箱尚未验证，某些功能可能受限"
                },
                "action_required": !user.is_verified,
                "action_url": if !user.is_verified {
                    Some(format!("{}/verify-email", app_state.config.auth_service_url))
                } else {
                    None
                }
            }
        }
    })))
}
