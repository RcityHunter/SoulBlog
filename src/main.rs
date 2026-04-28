use std::sync::Arc;
use axum::{
    extract::{Query, State},
    response::{Html, IntoResponse},
    routing::{Router, get, post},
    Extension,
    http::{Method, HeaderValue, header::SET_COOKIE},
    middleware,
};
use serde::Deserialize;
use tower_http::{
    cors::{CorsLayer, Any},
    compression::CompressionLayer,
    trace::TraceLayer,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use tracing::{info, warn, error};
use tokio::time::{interval, Duration};

mod routes;
mod models;
mod services;
mod config;
mod error;
mod utils;
mod state;
mod agent;

#[cfg(feature = "metrics")]
mod metrics;

use crate::{
    config::Config,
    state::AppState,
    services::{
        Database,
        AuthService,
        ArticleService,
        UserService,
        CommentService,
        NotificationService,
        SearchService,
        MediaService,
        RecommendationService,
        PublicationService,
        BookmarkService,
        FollowService,
        SeriesService,
        AnalyticsService,
        SubscriptionService,
        PaymentService,
        RevenueService,
        StripeService,
        WebSocketService,
        RealtimeService,
        DomainService,
        domain::DomainConfig,
    },
    models::stripe::StripeConfig,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化日志
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("LOG_LEVEL").unwrap_or_else(|_| "rainbow_blog=debug,tower_http=debug".into())
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting Rainbow-Blog service...");

    // 加载配置
    dotenv::dotenv().ok();
    let config = Config::from_env()?;
    
    // 初始化数据库连接
    let db = Arc::new(match Database::new(&config).await {
        Ok(db) => {
            match db.verify_connection().await {
                Ok(_) => {
                    info!("Database connection established successfully");
                    if let Err(e) = db.initialize_schema().await {
                        warn!("Schema initialization warning: {}", e);
                    }
                    db
                }
                Err(e) => {
                    warn!("Database connection failed: {}", e);
                    info!("Attempting to auto-start database...");
                    
                    // 尝试自动启动数据库
                    if let Err(start_err) = auto_start_database(&config).await {
                        error!("Failed to auto-start database: {}. Original error: {}", start_err, e);
                        return Err(anyhow::anyhow!("Database connection failed"));
                    }
                    
                    // 重新尝试连接
                    let db = Database::new(&config).await?;
                    db.verify_connection().await?;
                    if let Err(e) = db.initialize_schema().await {
                        warn!("Schema initialization warning: {}", e);
                    }
                    info!("Database auto-started and connected successfully");
                    db
                }
            }
        }
        Err(e) => {
            error!("Failed to create database connection: {}", e);
            return Err(anyhow::anyhow!("Database initialization failed"));
        }
    });

    // 初始化所有服务
    let auth_service = AuthService::new(&config).await?;
    let article_service = ArticleService::new(db.clone()).await?;
    let user_service = UserService::new(db.clone()).await?;
    let comment_service = CommentService::new(db.clone()).await?;
    let notification_service = NotificationService::new(db.clone(), &config).await?;
    let search_service = SearchService::new(db.clone()).await?;
    let media_service = MediaService::new(&config, db.clone()).await?;
    let recommendation_service = RecommendationService::new(db.clone()).await?;
    let publication_service = PublicationService::new(db.clone()).await?;
    let bookmark_service = BookmarkService::new(db.clone()).await?;
    let follow_service = FollowService::new(db.clone(), notification_service.clone()).await?;
    let tag_service = crate::services::tag::TagService::new(db.clone()).await?;
    let series_service = SeriesService::new(db.clone()).await?;
    let analytics_service = AnalyticsService::new(db.clone()).await?;
    let stripe_service = StripeService::new(db.clone(), StripeConfig::default()).await?;
    let stripe_service_arc = Arc::new(stripe_service.clone());
    let subscription_service = SubscriptionService::new(db.clone(), stripe_service_arc.clone()).await?;
    let subscription_service_arc = Arc::new(subscription_service.clone());
    let payment_service = PaymentService::new(
        db.clone(),
        subscription_service_arc.clone(),
        stripe_service_arc.clone(),
    )
    .await?;
    let revenue_service = RevenueService::new(db.clone(), stripe_service_arc.clone()).await?;
    let websocket_service = WebSocketService::new(db.clone()).await?;
    let realtime_service = RealtimeService::new(Arc::new(websocket_service.clone()), Arc::new(notification_service.clone()));
    
    // Initialize domain service with default config
    let domain_config = DomainConfig {
        base_domain: config.base_domain.clone().unwrap_or_else(|| "platform.local".to_string()),
        dns_verification_timeout: 300, // 5 minutes
        ssl_provider_endpoint: config.ssl_provider_endpoint.clone(),
        ssl_provider_api_key: config.ssl_provider_api_key.clone(),
        auto_provision_ssl: config.auto_provision_ssl.unwrap_or(false),
        ssl_webhook_url: config.ssl_webhook_url.clone(),
    };
    let domain_service = DomainService::new(db.clone(), domain_config).await?;

    // 创建应用状态
    let app_state = Arc::new(AppState {
        config: config.clone(),
        db: (*db).clone(),
        auth_service,
        article_service,
        user_service,
        comment_service,
        notification_service,
        search_service,
        media_service,
        recommendation_service,
        publication_service,
        bookmark_service,
        follow_service,
        tag_service,
        series_service,
        analytics_service,
        subscription_service,
        payment_service,
        revenue_service,
        stripe_service,
        websocket_service,
        realtime_service,
        domain_service,
    });

    // 启动后台任务
    start_background_tasks(app_state.clone()).await;

    // 配置 CORS
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS])
        .allow_headers(Any)
        .allow_origin(
            config.cors_allowed_origins
                .split(',')
                .map(|origin| origin.parse::<HeaderValue>().unwrap())
                .collect::<Vec<_>>(),
        );

    // 构建应用路由
    let app = Router::new()
        // API routes with /api/blog/ prefix (traditional API access)
        .nest("/api/blog/auth", routes::auth::router())
        .nest("/api/blog/users", routes::users::router())
        .nest("/api/blog/articles", routes::articles::router())
        .nest("/api/blog/comments", routes::comments::router())
        .nest("/api/blog/tags", routes::tags::router())
        .nest("/api/blog/publications", routes::publications::router())
        .nest("/api/blog/search", routes::search::router())
        .nest("/api/blog/media", routes::media::router())
        .nest("/api/blog/stats", routes::stats::router())
        .nest("/api/blog/bookmarks", routes::bookmarks::router())
        .nest("/api/blog/follows", routes::follows::router())
        .nest("/api/blog/recommendations", routes::recommendations::router())
        .nest("/api/blog/series", routes::series::router())
        .nest("/api/blog/analytics", routes::analytics::router())
        .nest("/api/blog/subscriptions", routes::subscriptions::router())
        .nest("/api/blog/payments", routes::payments::router())
        .nest("/api/blog/revenue", routes::revenue::router())
        .nest("/api/blog/stripe", routes::stripe::router())
        .nest("/api/blog/ws", routes::websocket::router())
        .nest("/api/blog/domains", routes::domain::router())
        .nest("/api/blog/diagnostics", routes::diagnostics::router())
        .nest("/api/blog/notifications", routes::notifications::router())
        .nest("/api/blog/ai", routes::ai::router())
        
        // Health check endpoints (no domain context needed)
        .route("/health", get(health_check))
        .route("/sso", get(sso_bridge))
        
        // Agent API v1 (for OpenClaw / Agent integration)
        .nest("/agent/v1", agent::agent_router(app_state.clone()))
        
        // Domain-specific routes (work with custom domains and subdomains)
        // These routes are merged at the root level and rely on domain routing middleware
        // This must come after specific routes to avoid conflicts
        .merge(routes::publication_content::router())
        
        // Apply middleware layers (order matters - they are applied in reverse)
        .layer(cors)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        
        // Domain routing middleware should be applied early to set publication context
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            utils::middleware::domain_routing_middleware,
        ))
        
        // Debug middleware to log requests
        .layer(middleware::from_fn(|req: axum::http::Request<axum::body::Body>, next: axum::middleware::Next| async move {
            let path = req.uri().path().to_string();
            let method = req.method().clone();
            tracing::info!("Incoming request (before auth): {} {}", method, path);
            let res = next.run(req).await;
            tracing::info!("Response (after processing): {} {} - {:?}", method, path, res.status());
            res
        }))
        
        // Authentication middleware (can use publication context if available)
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            utils::middleware::auth_middleware,
        ))
        
        // Rate limiting
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            utils::middleware::rate_limit_middleware,
        ))
        
        // Logging and security
        .layer(middleware::from_fn(
            utils::middleware::request_logging_middleware,
        ))
        .layer(middleware::from_fn(
            utils::middleware::security_headers_middleware,
        ))
        .layer(middleware::from_fn(
            utils::middleware::request_id_middleware,
        ))
        
        .with_state(app_state);

    // 启动指标服务器（如果启用）
    #[cfg(feature = "metrics")]
    if config.metrics_enabled {
        let metrics_app = metrics::setup_metrics().await?;
        let metrics_addr = format!("0.0.0.0:{}", config.metrics_port);
        info!("Starting metrics server on {}", metrics_addr);
        
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(&metrics_addr).await.unwrap();
            axum::serve(listener, metrics_app).await.unwrap();
        });
    }

    // 启动主服务器
    let addr = format!("{}:{}", config.server_host, config.server_port);
    info!("Starting server on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> &'static str {
    "Rainbow-Blog is running!"
}

#[derive(Deserialize)]
struct SsoParams {
    token: Option<String>,
    next: Option<String>,
}

async fn sso_bridge(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SsoParams>,
) -> impl IntoResponse {
    let token = params.token.unwrap_or_default();
    if token.trim().is_empty() {
        return Html("missing token".to_string()).into_response();
    }
    let next = params
        .next
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| state.config.frontend_url.clone());
    let token_js = serde_json::to_string(&token).unwrap_or_else(|_| "\"\"".into());
    let next_js = serde_json::to_string(&next).unwrap_or_else(|_| "\"/\"".into());
    let html = format!(
        r#"<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>SSO Redirect</title>
  </head>
  <body>
    <script>
      const token = {token_js};
      const next = {next_js};
      try {{
        localStorage.setItem('jwt_token', token);
        localStorage.setItem('auth_token', token);
        localStorage.setItem('token', token);
      }} catch (e) {{
        // ignore storage errors
      }}
      window.location.replace(next);
    </script>
  </body>
</html>"#
    );
    let cookie = format!(
        "RB_BLOG_TOKEN={}; Path=/; Max-Age=2592000; HttpOnly; SameSite=Lax",
        token
    );
    (
        [(SET_COOKIE, HeaderValue::from_str(&cookie).unwrap_or_else(|_| HeaderValue::from_static("RB_BLOG_TOKEN=; Path=/; Max-Age=0; SameSite=Lax")))],
        Html(html),
    )
        .into_response()
}

async fn auto_start_database(config: &Config) -> anyhow::Result<()> {
    info!("Attempting to start SurrealDB...");
    
    // 尝试启动 SurrealDB 进程
    let output = tokio::process::Command::new("surreal")
        .args(&[
            "start",
            "--user", &config.database_username,
            "--pass", &config.database_password,
            "memory",
        ])
        .spawn();

    match output {
        Ok(_) => {
            info!("SurrealDB started successfully");
            // 等待数据库启动
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok(())
        }
        Err(e) => {
            error!("Failed to start SurrealDB: {}", e);
            Err(anyhow::anyhow!("Failed to start database"))
        }
    }
}

async fn start_background_tasks(app_state: Arc<AppState>) {
    info!("Starting background tasks...");

    // 推荐系统更新任务（可通过 ENABLE_RECOMMENDATION_TASKS 开关关闭）
    if app_state.config.enable_recommendation_tasks {
        let recommendation_state = app_state.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(
                recommendation_state.config.recommendation_update_interval
            ));
            
            loop {
                interval.tick().await;
                if let Err(e) = recommendation_state.recommendation_service.update_recommendations().await {
                    error!("Failed to update recommendations: {}", e);
                }
            }
        });
    } else {
        info!("Recommendation background task is disabled by ENABLE_RECOMMENDATION_TASKS=false");
    }

    // 统计数据聚合任务
    let stats_state = app_state.clone();
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(3600)); // 每小时执行一次
        
        loop {
            interval.tick().await;
            if let Err(e) = stats_state.article_service.aggregate_daily_stats().await {
                error!("Failed to aggregate daily stats: {}", e);
            }
        }
    });

    // 清理过期会话任务
    let auth_state = app_state.clone();
    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(3600)); // 每小时执行一次
        
        loop {
            interval.tick().await;
            if let Err(e) = auth_state.auth_service.cleanup_expired_sessions().await {
                error!("Failed to cleanup expired sessions: {}", e);
            }
        }
    });

    info!("Background tasks started successfully");
}
