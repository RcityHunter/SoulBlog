use crate::{error::AppError, services::AuthService, services::auth::User, state::AppState};
use chrono::{Utc, TimeZone};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode, Request},
    middleware::Next,
    response::Response,
    body::Body,
};
use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed, keyed::DashMapStateStore},
    Quota, RateLimiter,
};
use std::{
    net::SocketAddr,
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};
use tracing::{debug, warn, info};
use tokio::sync::OnceCell;

type KeyedRateLimiter = RateLimiter<String, DashMapStateStore<String>, DefaultClock>;
static RATE_LIMITER: OnceCell<KeyedRateLimiter> = OnceCell::const_new();

/// 认证中间件
pub async fn auth_middleware(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, AppError> {
    let path = request.uri().path();
    info!("Auth middleware processing request to: {}", path);
    
    // 将认证服务和用户服务添加到请求扩展中，供后续处理器使用
    request.extensions_mut().insert(app_state.auth_service.clone());
    request.extensions_mut().insert(app_state.user_service.clone());
    
    // 支持 Authorization 头和 SSO Cookie，两者任选其一
    if let Some(token) = extract_bearer_token(&headers) {
        // 验证 JWT
        match app_state.auth_service.verify_jwt(&token) {
            Ok(claims) => {
                // 尝试获取用户信息
                match app_state
                    .auth_service
                    .get_user_from_rainbow_auth(&claims.sub, &token)
                    .await
                {
                    Ok(user) => {
                        debug!("Authenticated user via Rainbow-Auth: {} ({})", user.id, user.email);
                        let _ = app_state.user_service.get_or_create_profile(
                            &user.id, &user.email, user.is_verified,
                            user.username.clone(), user.display_name.clone(),
                        ).await;
                        request.extensions_mut().insert(user);
                    }
                    Err(e) => {
                        // Rainbow-Auth not available — build User from JWT claims (native auth)
                        debug!("Rainbow-Auth unavailable ({}), falling back to JWT claims for user {}", e, claims.sub);
                        let email = claims.email.clone().unwrap_or_default();
                        let profile = app_state.user_service.get_or_create_profile(
                            &claims.sub, &email, false, None, None,
                        ).await.ok();
                        let (username, display_name, avatar_url) = match &profile {
                            Some(p) => (Some(p.username.clone()), Some(p.display_name.clone()), p.avatar_url.clone()),
                            None => (None, None, None),
                        };
                        let created_at = Utc.timestamp_opt(claims.iat, 0).single().unwrap_or_else(Utc::now);
                        let user = User {
                            id: claims.sub.clone(),
                            email: email.clone(),
                            username,
                            display_name,
                            avatar_url,
                            roles: vec!["user".to_string()],
                            permissions: vec![
                                "article.read".to_string(), "article.write".to_string(),
                                "article.create".to_string(), "comment.read".to_string(),
                                "comment.create".to_string(), "user.read_profile".to_string(),
                                "user.update_profile".to_string(),
                            ],
                            is_verified: true,  // native-auth users are verified by registration
                            created_at,
                        };
                        info!("Native auth user resolved: {} ({})", user.id, user.email);
                        request.extensions_mut().insert(user);
                    }
                }
            }
            Err(e) => {
                debug!("JWT verification failed: {}", e);
                // 不返回错误，让请求继续处理（作为未认证请求）
            }
        }
    }

    Ok(next.run(request).await)
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    if let Some(auth_header) = headers.get("authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                let token = token.trim();
                if !token.is_empty() {
                    return Some(token.to_string());
                }
            }
        }
    }

    let cookie_header = headers.get("cookie")?.to_str().ok()?;
    for part in cookie_header.split(';') {
        let mut kv = part.trim().splitn(2, '=');
        let key = kv.next()?.trim();
        let value = kv.next().unwrap_or("").trim();
        let is_token_key = matches!(key, "RB_BLOG_TOKEN" | "jwt_token" | "auth_token" | "token");
        if is_token_key && !value.is_empty() {
            return Some(value.to_string());
        }
    }

    None
}

/// 速率限制中间件
pub async fn rate_limit_middleware(
    State(app_state): State<Arc<AppState>>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, AppError> {
    // 获取或创建速率限制器
    let rate_limiter = RATE_LIMITER.get_or_init(|| async {
        let quota = Quota::per_minute(NonZeroU32::new(app_state.config.rate_limit_requests).unwrap())
            .allow_burst(NonZeroU32::new(10).unwrap());
        RateLimiter::dashmap(quota)
    }).await;

    // 获取客户端 IP
    let client_ip = get_client_ip(&request);
    
    // 检查速率限制
    match rate_limiter.check_key(&client_ip) {
        Ok(_) => {
            debug!("Rate limit check passed for IP: {}", client_ip);
            Ok(next.run(request).await)
        }
        Err(_) => {
            warn!("Rate limit exceeded for IP: {}", client_ip);
            Err(AppError::RateLimitExceeded)
        }
    }
}

/// 请求日志中间件
pub async fn request_logging_middleware(
    request: Request<Body>,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let client_ip = get_client_ip(&request);
    
    let start_time = std::time::Instant::now();
    
    debug!("Incoming request: {} {} from {}", method, uri, client_ip);
    
    let response = next.run(request).await;
    
    let elapsed = start_time.elapsed();
    let status = response.status();
    
    info!(
        "Request completed: {} {} {} - {}ms",
        method,
        uri,
        status.as_u16(),
        elapsed.as_millis()
    );
    
    response
}

/// CORS 中间件（如果需要自定义逻辑）
pub async fn cors_middleware(
    request: Request<Body>,
    next: Next,
) -> Response {
    let origin = request
        .headers()
        .get("origin")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("*")
        .to_string();
    
    let mut response = next.run(request).await;
    
    // 添加 CORS 头
    let headers = response.headers_mut();
    headers.insert("access-control-allow-origin", origin.parse().unwrap());
    headers.insert("access-control-allow-methods", "GET, POST, PUT, DELETE, OPTIONS".parse().unwrap());
    headers.insert("access-control-allow-headers", "content-type, authorization".parse().unwrap());
    headers.insert("access-control-max-age", "3600".parse().unwrap());
    
    response
}

/// 安全头中间件
pub async fn security_headers_middleware(
    request: Request<Body>,
    next: Next,
) -> Response {
    let is_https = is_https_request(&request);
    let mut response = next.run(request).await;
    
    let headers = response.headers_mut();
    
    // 安全相关头
    headers.insert("x-content-type-options", "nosniff".parse().unwrap());
    headers.insert("x-frame-options", "DENY".parse().unwrap());
    headers.insert("x-xss-protection", "1; mode=block".parse().unwrap());
    headers.insert("referrer-policy", "strict-origin-when-cross-origin".parse().unwrap());
    
    // 如果是 HTTPS 环境，添加 HSTS
    if is_https {
        headers.insert("strict-transport-security", "max-age=31536000; includeSubDomains".parse().unwrap());
    }
    
    response
}

/// 内容压缩中间件（已在 main.rs 中使用 tower-http 的 CompressionLayer）

/// 请求 ID 中间件
pub async fn request_id_middleware(
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    
    // 添加到请求扩展中
    request.extensions_mut().insert(RequestId(request_id.clone()));
    
    let mut response = next.run(request).await;
    
    // 添加到响应头中
    response.headers_mut().insert("x-request-id", request_id.parse().unwrap());
    
    response
}

/// 健康检查绕过中间件
pub async fn health_check_bypass_middleware(
    request: Request<Body>,
    next: Next,
) -> Response {
    // 健康检查端点绕过某些中间件
    if request.uri().path() == "/health" || request.uri().path() == "/" {
        return next.run(request).await;
    }
    
    next.run(request).await
}

// 辅助函数

/// 获取客户端 IP 地址
fn get_client_ip(request: &Request<Body>) -> String {
    // 尝试从各种头中获取真实 IP
    let headers = request.headers();
    
    // 检查常见的代理头
    if let Some(forwarded_for) = headers.get("x-forwarded-for") {
        if let Ok(ip_str) = forwarded_for.to_str() {
            if let Some(ip) = ip_str.split(',').next() {
                return ip.trim().to_string();
            }
        }
    }
    
    if let Some(real_ip) = headers.get("x-real-ip") {
        if let Ok(ip_str) = real_ip.to_str() {
            return ip_str.to_string();
        }
    }
    
    if let Some(forwarded) = headers.get("forwarded") {
        if let Ok(forwarded_str) = forwarded.to_str() {
            // 解析 Forwarded 头（简化版本）
            for part in forwarded_str.split(';') {
                if part.trim().starts_with("for=") {
                    let ip = part.trim().strip_prefix("for=").unwrap_or("");
                    return ip.trim_matches('"').to_string();
                }
            }
        }
    }
    
    // 如果都没有，使用连接信息（在实际部署中可能不可用）
    request
        .extensions()
        .get::<SocketAddr>()
        .map(|addr| addr.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// 检查请求是否为 HTTPS
fn is_https_request(request: &Request<Body>) -> bool {
    // 检查协议
    if request.uri().scheme_str() == Some("https") {
        return true;
    }
    
    // 检查代理头
    if let Some(proto) = request.headers().get("x-forwarded-proto") {
        if let Ok(proto_str) = proto.to_str() {
            return proto_str == "https";
        }
    }
    
    if let Some(https) = request.headers().get("x-forwarded-ssl") {
        if let Ok(https_str) = https.to_str() {
            return https_str == "on";
        }
    }
    
    false
}

// 类型定义

/// 请求 ID 包装器
#[derive(Debug, Clone)]
pub struct RequestId(pub String);

/// 速率限制配置
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub requests_per_minute: u32,
    pub burst_size: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_minute: 60,
            burst_size: 10,
        }
    }
}

/// Domain-based routing middleware
pub async fn domain_routing_middleware(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, AppError> {
    // Extract the host header
    if let Some(host_header) = headers.get("host") {
        if let Ok(host_str) = host_header.to_str() {
            // Clean the host (remove port if present)
            let host = host_str.split(':').next().unwrap_or(host_str);
            
            debug!("Processing request for host: {}", host);
            
            // Check if this is a custom domain or subdomain
            if let Some(publication_id) = app_state.domain_service.find_publication_by_domain(host).await.unwrap_or(None) {
                debug!("Found publication {} for domain {}", publication_id, host);
                
                // Get publication details
                match app_state.publication_service.get_publication(&publication_id, None).await {
                    Ok(Some(publication)) => {
                        // Add publication context to request extensions
                        request.extensions_mut().insert(PublicationContext {
                            publication_id: publication_id.clone(),
                            publication: publication.publication.clone(),
                            domain: host.to_string(),
                            is_custom_domain: !host.contains(&app_state.config.base_domain.clone().unwrap_or_default()),
                        });
                        
                        debug!("Added publication context for {}", publication.publication.name);
                    }
                    Ok(None) => {
                        debug!("Publication {} not found", publication_id);
                    }
                    Err(e) => {
                        warn!("Failed to fetch publication {}: {}", publication_id, e);
                    }
                }
            } else {
                debug!("No publication found for domain {}", host);
            }
        }
    }

    Ok(next.run(request).await)
}

/// Publication context for domain-based routing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicationContext {
    pub publication_id: String,
    pub publication: crate::models::publication::Publication,
    pub domain: String,
    pub is_custom_domain: bool,
}

/// Extractor for optional publication context
pub struct OptionalPublicationContext(pub Option<PublicationContext>);

#[async_trait::async_trait]
impl<S> axum::extract::FromRequestParts<S> for OptionalPublicationContext
where
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut axum::http::request::Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let context = parts.extensions.get::<PublicationContext>().cloned();
        Ok(OptionalPublicationContext(context))
    }
}

/// Extractor for required publication context
pub struct RequiredPublicationContext(pub PublicationContext);

#[async_trait::async_trait]
impl<S> axum::extract::FromRequestParts<S> for RequiredPublicationContext
where
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut axum::http::request::Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let context = parts.extensions.get::<PublicationContext>()
            .cloned()
            .ok_or_else(|| AppError::BadRequest("Publication context required for this endpoint".to_string()))?;
        Ok(RequiredPublicationContext(context))
    }
}

/// 可选认证提取器
pub struct OptionalAuth(pub Option<crate::services::auth::User>);

#[async_trait::async_trait]
impl<S> axum::extract::FromRequestParts<S> for OptionalAuth
where
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut axum::http::request::Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let user = parts.extensions.get::<crate::services::auth::User>().cloned();
        Ok(OptionalAuth(user))
    }
}
