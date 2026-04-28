use crate::{
    error::Result,
    services::auth::User,
    state::AppState,
};
use axum::{
    extract::{Path, Query, State},
    response::Json,
    routing::{delete, get, put},
    Extension, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct NotificationQuery {
    pub page: Option<i32>,
    pub limit: Option<i32>,
    pub unread_only: Option<bool>,
}

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(get_notifications))
        .route("/count", get(get_unread_count))
        .route("/read-all", put(mark_all_read))
        .route("/:id/read", put(mark_read))
        .route("/:id", delete(delete_notification))
}

/// GET /api/blog/notifications
async fn get_notifications(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
    Query(query): Query<NotificationQuery>,
) -> Result<Json<Value>> {
    let notifications = state
        .notification_service
        .get_user_notifications(
            &user.id,
            query.page,
            query.limit,
            query.unread_only.unwrap_or(false),
        )
        .await?;

    Ok(Json(json!({
        "success": true,
        "data": notifications
    })))
}

/// GET /api/blog/notifications/count
async fn get_unread_count(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
) -> Result<Json<Value>> {
    let count = state
        .notification_service
        .get_unread_count(&user.id)
        .await
        .unwrap_or(0);

    Ok(Json(json!({
        "success": true,
        "count": count
    })))
}

/// PUT /api/blog/notifications/read-all
async fn mark_all_read(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
) -> Result<Json<Value>> {
    state
        .notification_service
        .mark_all_as_read(&user.id)
        .await?;

    Ok(Json(json!({ "success": true })))
}

/// PUT /api/blog/notifications/:id/read
async fn mark_read(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    state
        .notification_service
        .mark_as_read(&id, &user.id)
        .await?;

    Ok(Json(json!({ "success": true })))
}

/// DELETE /api/blog/notifications/:id
async fn delete_notification(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<User>,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    state
        .notification_service
        .delete_notification(&id, &user.id)
        .await?;

    Ok(Json(json!({ "success": true })))
}
