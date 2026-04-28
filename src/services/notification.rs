use crate::{
    error::Result,
    services::Database,
    config::Config,
    models::notification::*,
};
use std::sync::Arc;
use uuid::Uuid;
use chrono::Utc;
use serde_json::json;
use tracing::debug;

#[derive(Clone)]
pub struct NotificationService {
    db: Arc<Database>,
    config: Config,
}

impl NotificationService {
    pub async fn new(db: Arc<Database>, config: &Config) -> Result<Self> {
        Ok(Self {
            db,
            config: config.clone(),
        })
    }

    pub async fn create_notification(&self, request: CreateNotificationRequest) -> Result<Notification> {
        let notification = Notification {
            id: Uuid::new_v4().to_string(),
            recipient_id: request.recipient_id,
            notification_type: format!("{:?}", request.notification_type),
            title: request.title,
            message: request.message,
            data: request.data,
            is_read: false,
            read_at: None,
            created_at: Utc::now(),
        };

        let created: Notification = self.db.create("notification", notification).await?;
        Ok(created)
    }

    pub async fn get_user_notifications(
        &self,
        user_id: &str,
        page: Option<i32>,
        limit: Option<i32>,
        unread_only: bool,
    ) -> Result<Vec<Notification>> {
        debug!("Getting notifications for user: {}", user_id);
        let page = page.unwrap_or(1).max(1) as usize;
        let limit = limit.unwrap_or(20).max(1).min(100) as usize;
        let offset = (page - 1) * limit;

        let query = if unread_only {
            r#"SELECT * FROM notification
               WHERE recipient_id = $user_id AND is_read = false
               ORDER BY created_at DESC
               LIMIT $limit START $offset"#
        } else {
            r#"SELECT * FROM notification
               WHERE recipient_id = $user_id
               ORDER BY created_at DESC
               LIMIT $limit START $offset"#
        };

        let mut response = self.db.query_with_params(
            query,
            json!({ "user_id": user_id, "limit": limit, "offset": offset }),
        ).await?;

        let notifications: Vec<Notification> = response.take(0).unwrap_or_default();
        Ok(notifications)
    }

    pub async fn mark_as_read(&self, notification_id: &str, user_id: &str) -> Result<()> {
        debug!("Marking notification {} as read for user {}", notification_id, user_id);
        let _ = self.db.query_with_params(
            "UPDATE notification SET is_read = true, read_at = time::now() WHERE id = $id AND recipient_id = $user_id",
            json!({ "id": notification_id, "user_id": user_id }),
        ).await;
        Ok(())
    }

    pub async fn mark_all_as_read(&self, user_id: &str) -> Result<()> {
        debug!("Marking all notifications as read for user {}", user_id);
        let _ = self.db.query_with_params(
            "UPDATE notification SET is_read = true, read_at = time::now() WHERE recipient_id = $user_id AND is_read = false",
            json!({ "user_id": user_id }),
        ).await;
        Ok(())
    }

    pub async fn get_unread_count(&self, user_id: &str) -> Result<i64> {
        let mut response = self.db.query_with_params(
            "SELECT count() FROM notification WHERE recipient_id = $user_id AND is_read = false GROUP ALL",
            json!({ "user_id": user_id }),
        ).await?;

        let rows: Vec<serde_json::Value> = response.take(0).unwrap_or_default();
        Ok(rows.first()
            .and_then(|v| v.get("count"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0))
    }

    pub async fn delete_notification(&self, notification_id: &str, user_id: &str) -> Result<()> {
        let _ = self.db.query_with_params(
            "DELETE FROM notification WHERE id = $id AND recipient_id = $user_id",
            json!({ "id": notification_id, "user_id": user_id }),
        ).await;
        Ok(())
    }
}
