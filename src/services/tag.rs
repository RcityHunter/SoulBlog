use crate::{
    error::{AppError, Result},
    models::tag::*,
    services::Database,
    utils::slug,
};
use chrono::Utc;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};
use validator::Validate;
use uuid::Uuid;

#[derive(Clone)]
pub struct TagService {
    db: Arc<Database>,
}

impl TagService {
    pub async fn new(db: Arc<Database>) -> Result<Self> {
        Ok(Self { db })
    }

    pub async fn create_tag(&self, request: CreateTagRequest) -> Result<Tag> {
        debug!("Creating tag: {}", request.name);

        request
            .validate()
            .map_err(|e| AppError::ValidatorError(e))?;

        // Check if tag name already exists
        let mut response = self.db.query_with_params(
            r#"
                SELECT * FROM tag 
                WHERE name = $name
            "#,
            json!({
                "name": &request.name
            })
        ).await?;
        let existing: Vec<Tag> = response.take(0)?;

        if !existing.is_empty() {
            return Err(AppError::Conflict(
                format!("Tag '{}' already exists", request.name),
            ));
        }

        let tag = Tag {
            id: Uuid::new_v4().to_string(),
            name: request.name.clone(),
            slug: slug::generate_slug(&request.name),
            description: request.description,
            follower_count: 0,
            article_count: 0,
            is_featured: false,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let created: Tag = self.db.create("tag", tag).await?;
        
        info!("Created tag: {} ({})", created.name, created.id);
        Ok(created)
    }

    pub async fn get_tags(&self, query: TagQuery) -> Result<Vec<Tag>> {
        debug!("Getting tags with query: {:?}", query);

        let page = query.page.unwrap_or(1).max(1);
        let limit = query.limit.unwrap_or(20).min(100);
        let offset = (page - 1) * limit;
        let mut sql = String::from(
            "SELECT \
                meta::id(id) AS id, \
                name, \
                slug, \
                description, \
                0 AS follower_count, \
                0 AS article_count, \
                is_featured, \
                IF created_at = NONE { time::now() } ELSE { created_at } END AS created_at, \
                IF updated_at = NONE { time::now() } ELSE { updated_at } END AS updated_at \
             FROM tag"
        );
        let mut conditions: Vec<String> = Vec::new();
        let mut params = serde_json::Map::new();

        if let Some(search) = &query.search {
            conditions.push("(name CONTAINS $search OR description CONTAINS $search)".to_string());
            params.insert("search".to_string(), json!(search));
        }

        if query.featured_only.unwrap_or(false) {
            conditions.push("is_featured = true".to_string());
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        match query.sort_by.as_deref() {
            Some("popular") => sql.push_str(" ORDER BY article_count DESC"),
            Some("name") => sql.push_str(" ORDER BY name ASC"),
            Some("created_at") => sql.push_str(" ORDER BY created_at DESC"),
            _ => sql.push_str(" ORDER BY article_count DESC"),
        }

        sql.push_str(" LIMIT $limit START $offset");
        params.insert("limit".to_string(), json!(limit));
        params.insert("offset".to_string(), json!(offset));

        let mut response = self
            .db
            .query_with_params(&sql, json!(params))
            .await?;
        let mut tags: Vec<Tag> = response.take(0)?;

        self.populate_tag_counts(&mut tags).await?;

        match query.sort_by.as_deref() {
            Some("name") => tags.sort_by(|a, b| a.name.cmp(&b.name)),
            Some("created_at") => tags.sort_by(|a, b| b.created_at.cmp(&a.created_at)),
            _ => tags.sort_by(|a, b| b.article_count.cmp(&a.article_count)),
        }

        Ok(tags)
    }

    pub async fn get_tag_by_id(&self, tag_id: &str) -> Result<Option<Tag>> {
        let tag: Option<Tag> = self.db.get_by_id("tag", tag_id).await?;
        Ok(tag)
    }

    pub async fn get_tag_by_slug(&self, slug: &str) -> Result<Option<Tag>> {
        let sql = r#"
            SELECT
                meta::id(id) AS id,
                name,
                slug,
                description,
                0 AS follower_count,
                0 AS article_count,
                is_featured,
                IF created_at = NONE { time::now() } ELSE { created_at } END AS created_at,
                IF updated_at = NONE { time::now() } ELSE { updated_at } END AS updated_at
            FROM tag WHERE slug = $slug LIMIT 1
        "#;
        let mut response = self.db.query_with_params(sql, json!({"slug": slug})).await?;
        let mut tags: Vec<Tag> = response.take(0)?;

        if let Some(tag) = tags.get_mut(0) {
            self.populate_tag_counts(std::slice::from_mut(tag)).await?;
        }

        Ok(tags.into_iter().next())
    }

    pub async fn update_tag(&self, tag_id: &str, request: UpdateTagRequest) -> Result<Tag> {
        debug!("Updating tag: {}", tag_id);

        request
            .validate()
            .map_err(|e| AppError::ValidatorError(e))?;

        let tag: Tag = self
            .db
            .get_by_id("tag", tag_id)
            .await?
            .ok_or_else(|| AppError::NotFound("Tag not found".to_string()))?;

        // Check if new name conflicts with existing tag
        if let Some(ref new_name) = request.name {
            if new_name != &tag.name {
                let mut response = self.db.query_with_params(
                    "SELECT * FROM tag WHERE name = $name AND id != $id",
                    json!({
                        "name": new_name,
                        "id": tag_id
                    })
                ).await?;
                let existing: Vec<Tag> = response.take(0)?;

                if !existing.is_empty() {
                    return Err(AppError::Conflict(
                        format!("Tag '{}' already exists", new_name),
                    ));
                }
            }
        }

        let mut updates = json!({
            "updated_at": Utc::now(),
        });

        if let Some(name) = request.name {
            updates["name"] = json!(name.clone());
            updates["slug"] = json!(slug::generate_slug(&name));
        }
        
        if let Some(description) = request.description {
            updates["description"] = json!(description);
        }
        
        if let Some(is_featured) = request.is_featured {
            updates["is_featured"] = json!(is_featured);
        }

        let updated: Tag = self.db.update_by_id_with_json("tag", tag_id, updates).await?.ok_or_else(|| AppError::internal("Failed to update tag"))?;
        
        Ok(updated)
    }

    pub async fn delete_tag(&self, tag_id: &str) -> Result<()> {
        debug!("Deleting tag: {}", tag_id);

        let tag: Tag = self
            .db
            .get_by_id("tag", tag_id)
            .await?
            .ok_or_else(|| AppError::NotFound("Tag not found".to_string()))?;

        // Check if tag is used by any articles
        if tag.article_count > 0 {
            return Err(AppError::Conflict(
                format!("Cannot delete tag '{}' as it is used by {} articles", tag.name, tag.article_count),
            ));
        }

        // Delete all user follows for this tag
        self.db.query_with_params(
            "DELETE user_tag_follow WHERE tag_id = $tag_id",
            json!({ "tag_id": tag_id })
        ).await?;

        // Delete the tag
        self.db.delete_by_id("tag", tag_id).await?;
        
        info!("Deleted tag: {} ({})", tag.name, tag_id);
        Ok(())
    }

    pub async fn add_tags_to_article(&self, article_id: &str, tag_ids: Vec<String>) -> Result<()> {
        debug!("Adding {} tags to article: {}", tag_ids.len(), article_id);

        let normalized_article_id = normalize_surreal_id(article_id);

        for tag_id in tag_ids {
            // Check if tag exists
            let tag: Option<Tag> = self.db.get_by_id("tag", &tag_id).await?;
            if tag.is_none() {
                return Err(AppError::NotFound(format!("Tag {} not found", tag_id)));
            }

            let normalized_tag_id = normalize_surreal_id(&tag_id);

            // Check if association already exists
            let mut response = self.db.query_with_params(
                r#"
                    SELECT * FROM article_tag 
                    WHERE article_id = type::thing('article', $aid) 
                    AND tag_id = type::thing('tag', $tid)
                "#,
                json!({
                    "aid": normalized_article_id,
                    "tid": normalized_tag_id
                })
            ).await?;
            let existing: Vec<ArticleTag> = response.take(0)?;

            if existing.is_empty() {
                let create_query = r#"
                    CREATE article_tag SET 
                        article_id = type::thing('article', $aid),
                        tag_id = type::thing('tag', $tid)
                "#;
                self.db
                    .query_with_params(create_query, json!({
                        "aid": normalized_article_id,
                        "tid": normalized_tag_id
                    }))
                    .await?;

                // Update tag article count
                self.update_tag_article_count(&normalized_tag_id).await?;
            }
        }

        Ok(())
    }

    pub async fn remove_tags_from_article(
        &self,
        article_id: &str,
        tag_ids: Vec<String>,
    ) -> Result<()> {
        debug!("Removing {} tags from article: {}", tag_ids.len(), article_id);

        let normalized_article_id = normalize_surreal_id(article_id);

        for tag_id in tag_ids {
            let normalized_tag_id = normalize_surreal_id(&tag_id);

            self.db.query_with_params(
                r#"
                    DELETE article_tag 
                    WHERE article_id = type::thing('article', $aid) 
                    AND tag_id = type::thing('tag', $tid)
                "#,
                json!({
                    "aid": normalized_article_id,
                    "tid": normalized_tag_id
                })
            ).await?;

            // Update tag article count
            self.update_tag_article_count(&normalized_tag_id).await?;
        }

        Ok(())
    }

    pub async fn get_article_tags(&self, article_id: &str) -> Result<Vec<Tag>> {
        let query = r#"
            SELECT t.* FROM tag t
            JOIN article_tag at ON t.id = at.tag_id
            WHERE at.article_id = $article_id
            ORDER BY t.name
        "#;

        let mut response = self.db.query_with_params(query, json!({
            "article_id": article_id
        })).await?;
        let tags: Vec<Tag> = response.take(0)?;

        Ok(tags)
    }

    pub async fn follow_tag(&self, tag_id: &str, user_id: &str) -> Result<()> {
        debug!("User {} following tag: {}", user_id, tag_id);

        // Check if tag exists
        let tag: Option<Tag> = self.db.get_by_id("tag", tag_id).await?;
        if tag.is_none() {
            return Err(AppError::NotFound("Tag not found".to_string()));
        }

        // Check if already following
        let mut response = self.db.query_with_params(
            r#"
                SELECT * FROM user_tag_follow 
                WHERE user_id = $user_id 
                AND tag_id = $tag_id
            "#,
            json!({
                "user_id": user_id,
                "tag_id": tag_id
            })
        ).await?;
        let existing: Vec<UserTagFollow> = response.take(0)?;

        if !existing.is_empty() {
            return Err(AppError::Conflict("Already following this tag".to_string()));
        }

        let follow = UserTagFollow {
            id: Uuid::new_v4().to_string(),
            user_id: user_id.to_string(),
            tag_id: tag_id.to_string(),
            created_at: Utc::now(),
        };

        self.db.create("user_tag_follow", follow).await?;

        // Update tag follower count
        self.update_tag_follower_count(tag_id).await?;

        Ok(())
    }

    pub async fn unfollow_tag(&self, tag_id: &str, user_id: &str) -> Result<()> {
        debug!("User {} unfollowing tag: {}", user_id, tag_id);

        self.db.query_with_params(
            r#"
                DELETE user_tag_follow 
                WHERE user_id = $user_id 
                AND tag_id = $tag_id
            "#,
            json!({
                "user_id": user_id,
                "tag_id": tag_id
            })
        ).await?;

        // Update tag follower count
        self.update_tag_follower_count(tag_id).await?;

        Ok(())
    }

    pub async fn get_user_followed_tags(&self, user_id: &str) -> Result<Vec<Tag>> {
        let query = r#"
            SELECT t.* FROM tag t
            JOIN user_tag_follow utf ON t.id = utf.tag_id
            WHERE utf.user_id = $user_id
            ORDER BY utf.created_at DESC
        "#;

        let mut response = self.db.query_with_params(query, json!({
            "user_id": user_id
        })).await?;
        let tags: Vec<Tag> = response.take(0)?;

        Ok(tags)
    }

    /// 获取用户关注的标签ID集合（标准化为字符串形式，兼容 Surreal 记录格式）
    pub async fn get_followed_tag_id_set(&self, user_id: &str) -> Result<std::collections::HashSet<String>> {
        let sql = r#"
            SELECT string::replace(string::replace(type::string(tag_id), '⟨', ''), '⟩', '') AS tid
            FROM user_tag_follow
            WHERE user_id = $user_id
        "#;

        let mut resp = self.db.query_with_params(sql, json!({"user_id": user_id})).await?;
        let rows: Vec<Value> = resp.take(0)?;
        let set = rows.into_iter()
            .filter_map(|v| v.get("tid").and_then(|x| x.as_str()).map(|s| s.to_string()))
            .collect();
        Ok(set)
    }

    pub async fn get_tags_with_follow_status(
        &self,
        user_id: Option<&str>,
        tag_ids: Vec<String>,
    ) -> Result<Vec<TagWithFollowStatus>> {
        // 兼容 Surreal record(id) 与传入字符串 id（例如 "tag:uuid"）的比较
        // 使用 type::string(id) 进行 IN 过滤
        let mut response = self.db.query_with_params(
            "SELECT * FROM tag WHERE string::replace(string::replace(type::string(id), '⟨', ''), '⟩', '') IN $tag_ids",
            json!({ "tag_ids": tag_ids.clone() })
        ).await?;
        let tags: Vec<Tag> = response.take(0)?;

        let mut result = Vec::new();

        if let Some(uid) = user_id {
            // 同理，user_tag_follow.tag_id 是 record(tag)，改用 type::string(tag_id) 进行 IN 过滤
            let mut response = self.db.query_with_params(
                r#"
                    SELECT * FROM user_tag_follow 
                    WHERE user_id = $user_id 
                    AND string::replace(string::replace(type::string(tag_id), '⟨', ''), '⟩', '') IN $tag_ids
                "#,
                json!({
                    "user_id": uid,
                    "tag_ids": tag_ids
                })
            ).await?;
            let followed: Vec<UserTagFollow> = response.take(0)?;

            let followed_set: std::collections::HashSet<String> =
                followed.into_iter().map(|f| f.tag_id).collect();

            for tag in tags {
                result.push(TagWithFollowStatus {
                    is_following: followed_set.contains(&tag.id),
                    tag,
                });
            }
        } else {
            for tag in tags {
                result.push(TagWithFollowStatus {
                    tag,
                    is_following: false,
                });
            }
        }

        Ok(result)
    }

    async fn populate_tag_counts(&self, tags: &mut [Tag]) -> Result<()> {
        if tags.is_empty() {
            return Ok(());
        }

        let mut normalized_ids: Vec<String> = tags
            .iter()
            .map(|tag| normalize_surreal_id(&tag.id))
            .collect();
        normalized_ids.sort();
        normalized_ids.dedup();

        let article_counts = self
            .fetch_relation_counts("article_tag", "tag_id", &normalized_ids)
            .await?;
        let follower_counts = self
            .fetch_relation_counts("user_tag_follow", "tag_id", &normalized_ids)
            .await?;

        for tag in tags.iter_mut() {
            let key = normalize_surreal_id(&tag.id);
            tag.article_count = *article_counts.get(&key).unwrap_or(&0);
            tag.follower_count = *follower_counts.get(&key).unwrap_or(&0);
        }

        Ok(())
    }

    async fn fetch_relation_counts(
        &self,
        table: &str,
        field: &str,
        ids: &[String],
    ) -> Result<HashMap<String, i64>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let field_expr = format!(
            "string::split(string::replace(string::replace(type::string({}), '⟨', ''), '⟩', ''), ':')[1]",
            field
        );
        let sql = format!(
            "SELECT {expr} AS tid FROM {table} WHERE {expr} IN $ids",
            expr = field_expr,
            table = table
        );

        let mut response = self
            .db
            .query_with_params(&sql, json!({ "ids": ids }))
            .await?;

        let rows: Vec<Value> = response.take(0)?;
        let mut map = HashMap::new();
        for row in rows {
            if let Some(raw_tid) = row.get("tid") {
                if let Some(tid) = extract_id_from_value(raw_tid) {
                    *map.entry(tid).or_insert(0) += 1;
                }
            }
        }

        Ok(map)
    }

    async fn update_tag_article_count(&self, tag_id: &str) -> Result<()> {
        let normalized = normalize_surreal_id(tag_id);
        let counts = self
            .fetch_relation_counts("article_tag", "tag_id", &[normalized.clone()])
            .await?;
        let count = *counts.get(&normalized).unwrap_or(&0);

        let update_sql = r#"UPDATE type::thing('tag', $tag_id) SET article_count = $count"#;

        self.db
            .query_with_params(update_sql, json!({
                "count": count,
                "tag_id": normalized
            }))
            .await?;

        Ok(())
    }

    async fn update_tag_follower_count(&self, tag_id: &str) -> Result<()> {
        let normalized = normalize_surreal_id(tag_id);
        let counts = self
            .fetch_relation_counts("user_tag_follow", "tag_id", &[normalized.clone()])
            .await?;
        let count = *counts.get(&normalized).unwrap_or(&0);

        let update_sql = r#"UPDATE type::thing('tag', $tag_id) SET follower_count = $count"#;

        self.db
            .query_with_params(update_sql, json!({
                "count": count,
                "tag_id": normalized
            }))
            .await?;

        Ok(())
    }
}

fn normalize_surreal_id(id: &str) -> String {
    fn try_from_json_str(s: &str) -> Option<String> {
        serde_json::from_str::<Value>(s)
            .ok()
            .and_then(|v| extract_id_from_json_value(&v))
    }

    fn extract_id_from_json_value(value: &Value) -> Option<String> {
        match value {
            Value::String(s) => Some(s.clone()),
            Value::Object(map) => {
                if let Some(Value::String(s)) = map.get("String") {
                    return Some(s.clone());
                }
                if let Some(Value::String(s)) = map.get("id") {
                    return Some(s.clone());
                }
                if let Some(Value::Object(inner)) = map.get("id") {
                    if let Some(Value::String(s)) = inner.get("String") {
                        return Some(s.clone());
                    }
                }
                None
            }
            _ => None,
        }
    }

    let trimmed = id.trim();
    if let Some(res) = try_from_json_str(trimmed) {
        return res;
    }

    let cleaned = trimmed.replace('⟨', "").replace('⟩', "");
    if let Some(res) = try_from_json_str(&cleaned) {
        return res;
    }

    if let Some((_, rest)) = cleaned.split_once(':') {
        if let Some(res) = try_from_json_str(rest) {
            return res;
        }
        return rest.trim_matches('"').to_string();
    }

    cleaned.trim_matches('"').to_string()
}

fn extract_id_from_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) => Some(normalize_surreal_id(s)),
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(s)) = map.get("String") {
                return Some(normalize_surreal_id(s));
            }
            if let Some(serde_json::Value::Object(inner)) = map.get("id") {
                if let Some(serde_json::Value::String(s)) = inner.get("String") {
                    return Some(normalize_surreal_id(s));
                }
            }
            None
        }
        _ => None,
    }
}
