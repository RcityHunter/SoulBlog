use crate::{
    error::{AppError, Result},
    models::article::*,
    services::Database,
    utils::{markdown::MarkdownProcessor, slug},
};
use chrono::Utc;
use serde_json::{json, Value as JsonValue};
use tracing::{info, warn, error, debug};
use validator::Validate;
use std::collections::HashMap;
use std::sync::Arc;
use serde::de::DeserializeOwned;
use soulcore::prelude::Thing;
use surrealdb::types::Value as SurrealValue;
use uuid::Uuid;

#[derive(Clone)]
pub struct ArticleService {
    db: Arc<Database>,
    markdown_processor: MarkdownProcessor,
}

fn normalize_surreal_id(id: &str) -> String {
    fn try_from_json_str(s: &str) -> Option<String> {
        serde_json::from_str::<serde_json::Value>(s)
            .ok()
            .and_then(|v| extract_id_from_json_value(&v))
    }

    fn extract_id_from_json_value(value: &serde_json::Value) -> Option<String> {
        match value {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Object(map) => {
                if let Some(serde_json::Value::String(s)) = map.get("String") {
                    return Some(s.clone());
                }
                if let Some(serde_json::Value::String(s)) = map.get("id") {
                    return Some(s.clone());
                }
                if let Some(serde_json::Value::Object(inner)) = map.get("id") {
                    if let Some(serde_json::Value::String(s)) = inner.get("String") {
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

    // Strip SurrealDB escaping characters (angle brackets and backticks)
    let cleaned = trimmed
        .replace('⟨', "").replace('⟩', "")
        .replace('`', "");
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

pub(crate) fn normalize_surreal_json(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(mut map) => {
            if let Some(v) = map.remove("Strand") {
                return normalize_surreal_json(v);
            }
            if let Some(v) = map.remove("String") {
                return normalize_surreal_json(v);
            }
            if let Some(v) = map.remove("Bool") {
                return normalize_surreal_json(v);
            }
            if let Some(v) = map.remove("Array") {
                return normalize_surreal_json(v);
            }
            if map.remove("None").is_some() {
                return JsonValue::Null;
            }
            if map.remove("Null").is_some() {
                return JsonValue::Null;
            }
            if let Some(v) = map.remove("Some") {
                return normalize_surreal_json(v);
            }
            if map.len() == 1 {
                if let Some(v) = map.remove("value") {
                    return normalize_surreal_json(v);
                }
            }
            if let Some(v) = map.remove("Datetime") {
                return normalize_surreal_json(v);
            }
            if let Some(v) = map.remove("DateTime") {
                return normalize_surreal_json(v);
            }
            if let Some(v) = map.remove("Uuid") {
                return normalize_surreal_json(v);
            }
            if let Some(v) = map.remove("Object") {
                return normalize_surreal_json(v);
            }
            if let Some(v) = map.remove("Number") {
                if let JsonValue::Object(mut num) = v {
                    if let Some(i) = num.remove("Int") {
                        return normalize_surreal_json(i);
                    }
                    if let Some(f) = num.remove("Float") {
                        return normalize_surreal_json(f);
                    }
                    if let Some(d) = num.remove("Decimal") {
                        return normalize_surreal_json(d);
                    }
                }
                return JsonValue::Null;
            }
            if let Some(v) = map.remove("Thing") {
                if let JsonValue::Object(mut thing) = v {
                    let tb = thing.remove("tb").and_then(|v| v.as_str().map(|s| s.to_string()));
                    let id_val = thing.remove("id");
                    if let (Some(tb), Some(id_val)) = (tb, id_val) {
                        let id_str = match normalize_surreal_json(id_val) {
                            JsonValue::String(s) => s,
                            other => other.to_string(),
                        };
                        return JsonValue::String(format!("{}:{}", tb, id_str));
                    }
                }
            }
            let normalized = map
                .into_iter()
                .map(|(k, v)| (k, normalize_surreal_json(v)))
                .collect::<serde_json::Map<String, JsonValue>>();
            JsonValue::Object(normalized)
        }
        JsonValue::Array(arr) => {
            JsonValue::Array(arr.into_iter().map(normalize_surreal_json).collect())
        }
        other => other,
    }
}

fn surreal_to_json(value: SurrealValue) -> Option<JsonValue> {
    match serde_json::to_value(value) {
        Ok(v) => Some(normalize_surreal_json(v)),
        Err(err) => {
            warn!("Failed to convert Surreal value to JSON: {}", err);
            None
        }
    }
}

fn parse_articles_from_value_list(values: Vec<JsonValue>) -> Result<Vec<Article>> {
    let mut articles = Vec::new();
    for value in values {
        let normalized = normalize_surreal_json(value);
        let normalized = coerce_article_numeric_fields(normalized);
        match serde_json::from_value::<Article>(normalized) {
            Ok(article) => articles.push(article),
            Err(err) => {
                warn!("Skipping article due to deserialization error: {}", err);
            }
        }
    }
    Ok(articles)
}

fn coerce_article_numeric_fields(mut value: JsonValue) -> JsonValue {
    fn coerce_single_number(v: &mut JsonValue) {
        if let JsonValue::Array(arr) = v {
            if arr.len() != 1 {
                return;
            }

            let first = arr.remove(0);
            let coerced = match first {
                JsonValue::Number(_) => Some(first),
                JsonValue::String(s) => {
                    if let Ok(int_val) = s.parse::<i64>() {
                        Some(JsonValue::Number(int_val.into()))
                    } else if let Ok(float_val) = s.parse::<f64>() {
                        serde_json::Number::from_f64(float_val).map(JsonValue::Number)
                    } else {
                        None
                    }
                }
                JsonValue::Object(mut obj) => {
                    // Common shapes: {"count": 0} or {"value": 0}
                    if let Some(v) = obj.remove("count").or_else(|| obj.remove("value")) {
                        match v {
                            JsonValue::Number(_) => Some(v),
                            JsonValue::String(s) => {
                                if let Ok(int_val) = s.parse::<i64>() {
                                    Some(JsonValue::Number(int_val.into()))
                                } else if let Ok(float_val) = s.parse::<f64>() {
                                    serde_json::Number::from_f64(float_val).map(JsonValue::Number)
                                } else {
                                    None
                                }
                            }
                            other => Some(other),
                        }
                    } else {
                        None
                    }
                }
                other => Some(other),
            };

            if let Some(val) = coerced {
                *v = val;
            }
        }
    }

    if let JsonValue::Object(map) = &mut value {
        for key in [
            "reading_time",
            "word_count",
            "view_count",
            "clap_count",
            "comment_count",
            "bookmark_count",
            "share_count",
        ] {
            if let Some(v) = map.get_mut(key) {
                coerce_single_number(v);
            }
        }
    }

    value
}

fn parse_from_surreal_list<T: DeserializeOwned>(raw_list: Vec<SurrealValue>) -> Vec<T> {
    let mut items = Vec::new();
    for raw in raw_list {
        if let Some(json_value) = surreal_to_json(raw) {
            match serde_json::from_value::<T>(json_value) {
                Ok(item) => items.push(item),
                Err(err) => {
                    warn!("Skipping item due to deserialization error: {}", err);
                }
            }
        }
    }
    items
}

fn parse_from_json_list<T: DeserializeOwned>(raw_list: Vec<JsonValue>) -> Vec<T> {
    let mut items = Vec::new();
    for raw in raw_list {
        let normalized = normalize_surreal_json(raw);
        match serde_json::from_value::<T>(normalized) {
            Ok(item) => items.push(item),
            Err(err) => {
                warn!("Skipping item due to deserialization error: {}", err);
            }
        }
    }
    items
}

fn surreal_to_json_list(raw: SurrealValue) -> Vec<JsonValue> {
    if let Some(json_value) = surreal_to_json(raw) {
        match json_value {
            JsonValue::Array(arr) => arr,
            other => vec![other],
        }
    } else {
        Vec::new()
    }
}

impl ArticleService {
    pub async fn new(db: Arc<Database>) -> Result<Self> {
        let markdown_processor = MarkdownProcessor::new();

        Ok(Self {
            db,
            markdown_processor,
        })
    }

    /// 创建新文章
    pub async fn create_article(&self, author_id: &str, request: CreateArticleRequest) -> Result<Article> {
        debug!("Creating article for user: {}", author_id);

        // 验证输入
        request.validate()
            .map_err(|e| AppError::ValidatorError(e))?;

        // 创建文章对象
        let mut article = Article {
            id: Uuid::new_v4().to_string(),
            title: request.title,
            subtitle: request.subtitle,
            slug: String::new(), // 稍后生成
            content: request.content,
            content_html: String::new(), // 稍后生成
            excerpt: request.excerpt,
            cover_image_url: request.cover_image_url,
            author_id: author_id.to_string(),
            publication_id: request.publication_id,
            series_id: request.series_id,
            series_order: request.series_order,
            status: {
                // Explicit `status` field takes precedence over `save_as_draft` flag
                if let Some(s) = request.status.clone() {
                    s
                } else if request.save_as_draft.unwrap_or(true) {
                    ArticleStatus::Draft
                } else {
                    ArticleStatus::Published
                }
            },
            is_paid_content: request.is_paid_content.unwrap_or(false),
            is_featured: false,
            reading_time: 0, // 稍后计算
            word_count: 0, // 稍后计算
            view_count: 0,
            clap_count: 0,
            comment_count: 0,
            bookmark_count: 0,
            share_count: 0,
            seo_title: request.seo_title,
            seo_description: request.seo_description,
            seo_keywords: request.seo_keywords.unwrap_or_default(),
            metadata: serde_json::json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            published_at: None,
            last_edited_at: None,
            is_deleted: false,
            deleted_at: None,
        };

        // 生成唯一的 slug
        article.slug = self.generate_unique_slug(&article.title).await?;

        // 处理 Markdown 内容
        article.content_html = self.markdown_processor.to_html(&article.content);
        
        // 计算阅读时间和字数
        article.reading_time = self.markdown_processor.estimate_reading_time(&article.content);
        article.word_count = self.markdown_processor.count_words(&article.content) as i32;
        
        // 如果没有提供摘要，自动生成
        if article.excerpt.is_none() {
            article.excerpt = Some(self.markdown_processor.generate_excerpt(&article.content, 300));
        }

        // 如果没有封面图，尝试从内容中提取
        if article.cover_image_url.is_none() {
            article.cover_image_url = self.markdown_processor.extract_cover_image(&article.content);
        }

        // 如果是发布状态，设置发布时间
        if article.status == ArticleStatus::Published {
            article.published_at = Some(Utc::now());
        }

        let status_str = match article.status {
            ArticleStatus::Draft => "draft",
            ArticleStatus::Published => "published",
            ArticleStatus::Unlisted => "unlisted",
            ArticleStatus::Archived => "archived",
        };

        let mut params_map = serde_json::Map::new();
        params_map.insert("title".into(), json!(article.title));
        if let Some(subtitle) = article.subtitle.clone() {
            params_map.insert("subtitle".into(), json!(subtitle));
        }
        params_map.insert("slug".into(), json!(article.slug));
        params_map.insert("content".into(), json!(article.content));
        params_map.insert("content_html".into(), json!(article.content_html));
        if let Some(excerpt) = article.excerpt.clone() {
            params_map.insert("excerpt".into(), json!(excerpt));
        }
        if let Some(cover) = article.cover_image_url.clone() {
            params_map.insert("cover_image_url".into(), json!(cover));
        }
        params_map.insert("author_id".into(), json!(article.author_id));
        if let Some(publication_id) = article.publication_id.clone() {
            params_map.insert("publication_id".into(), json!(publication_id));
        }
        if let Some(series_id) = article.series_id.clone() {
            params_map.insert("series_id".into(), json!(series_id));
        }
        if let Some(series_order) = article.series_order {
            params_map.insert("series_order".into(), json!(series_order));
        }
        params_map.insert("status".into(), json!(status_str));
        params_map.insert("is_paid_content".into(), json!(article.is_paid_content));
        params_map.insert("is_featured".into(), json!(article.is_featured));
        params_map.insert("reading_time".into(), json!(article.reading_time));
        params_map.insert("word_count".into(), json!(article.word_count));
        params_map.insert("view_count".into(), json!(0));
        params_map.insert("clap_count".into(), json!(0));
        params_map.insert("comment_count".into(), json!(0));
        params_map.insert("bookmark_count".into(), json!(0));
        params_map.insert("share_count".into(), json!(0));
        if let Some(seo_title) = article.seo_title.clone() {
            params_map.insert("seo_title".into(), json!(seo_title));
        }
        if let Some(seo_description) = article.seo_description.clone() {
            params_map.insert("seo_description".into(), json!(seo_description));
        }
        params_map.insert("seo_keywords".into(), json!(article.seo_keywords));
        // created_at and updated_at are filled by DEFINE FIELD DEFAULT time::now()
        // published_at is set via UPDATE after CREATE if status == Published
        params_map.insert("is_deleted".into(), json!(false));
        if let Some(obj) = article.metadata.as_object() {
            if !obj.is_empty() {
                params_map.insert("metadata".into(), json!(article.metadata));
            }
        }

        let content_json = serde_json::Value::Object(params_map);
        let content_str = serde_json::to_string(&content_json)
            .map_err(|e| AppError::Serialization(e))?;
        let query = format!(
            "CREATE article:`{}` CONTENT {} RETURN *",
            article.id,
            content_str
        );
        debug!("Executing create query: {}", query);

        let mut response = self.db.query(&query).await?;
        let raw: SurrealValue = response.take(0)?;
        let raw_json = serde_json::to_value(raw)?;
        debug!("Create article raw response: {}", raw_json);
        let list_json = normalize_surreal_json(raw_json);
        let list = match list_json {
            JsonValue::Array(arr) => arr,
            other => vec![other],
        };
        let created_articles = parse_articles_from_value_list(list)?;
        let mut created_article = created_articles.into_iter().next()
            .ok_or_else(|| AppError::Internal("Failed to create article".to_string()))?;

        // If the article is created as published, ensure published_at is set with DB datetime.
        if created_article.status == ArticleStatus::Published && created_article.published_at.is_none() {
            let publish_query = format!(
                "UPDATE article:`{}` SET published_at = time::now(), updated_at = time::now() RETURN *",
                created_article.id
            );
            let mut publish_response = self.db.query(&publish_query).await?;
            let raw: SurrealValue = publish_response.take(0)?;
            let raw_json = serde_json::to_value(raw)?;
            let list_json = normalize_surreal_json(raw_json);
            let list = match list_json {
                JsonValue::Array(arr) => arr,
                other => vec![other],
            };
            let updated_articles = parse_articles_from_value_list(list)?;
            if let Some(updated) = updated_articles.into_iter().next() {
                created_article = updated;
            }
        }

        // 处理标签（如果有）
        if let Some(tags) = &request.tags {
            self.attach_tags_to_article(&created_article.id, tags).await?;
        }

        info!("Created article: {} by user: {}", created_article.id, author_id);
        Ok(created_article)
    }

    /// 更新文章
    pub async fn update_article(&self, article_id: &str, author_id: &str, request: UpdateArticleRequest) -> Result<Article> {
        debug!("Updating article: {} by user: {}", article_id, author_id);

        // 验证输入
        request.validate()
            .map_err(|e| AppError::ValidatorError(e))?;

        // 获取现有文章
        let mut article = self.get_article_by_id(article_id).await?
            .ok_or_else(|| AppError::NotFound("Article not found".to_string()))?;

        // 检查权限
        if article.author_id != author_id {
            return Err(AppError::Authorization("Only article author can update this article".to_string()));
        }

        // 更新字段
        let mut content_updated = false;
        
        if let Some(title) = request.title {
            if title != article.title {
                article.title = title;
                // 生成新的 slug
                article.slug = self.generate_unique_slug(&article.title).await?;
            }
        }

        if let Some(content) = request.content {
            article.content = content;
            article.content_html = self.markdown_processor.to_html(&article.content);
            article.reading_time = self.markdown_processor.estimate_reading_time(&article.content);
            article.word_count = self.markdown_processor.count_words(&article.content) as i32;
            content_updated = true;
        }
        
        if let Some(subtitle) = request.subtitle {
            article.subtitle = Some(subtitle);
        }

        if let Some(excerpt) = request.excerpt {
            article.excerpt = Some(excerpt);
        }

        if let Some(cover_image_url) = request.cover_image_url {
            article.cover_image_url = Some(cover_image_url);
        }

        if let Some(publication_id) = request.publication_id {
            article.publication_id = Some(publication_id);
        }

        if let Some(series_id) = request.series_id {
            article.series_id = Some(series_id);
        }
        
        if let Some(series_order) = request.series_order {
            article.series_order = Some(series_order);
        }

        if let Some(status) = request.status {
            if article.status != ArticleStatus::Published && status == ArticleStatus::Published {
                // 首次发布
                article.published_at = Some(Utc::now());
            }
            article.status = status;
        }

        if let Some(is_paid_content) = request.is_paid_content {
            article.is_paid_content = is_paid_content;
        }
        
        if let Some(seo_title) = request.seo_title {
            article.seo_title = Some(seo_title);
        }
        
        if let Some(seo_description) = request.seo_description {
            article.seo_description = Some(seo_description);
        }
        
        if let Some(seo_keywords) = request.seo_keywords {
            article.seo_keywords = seo_keywords;
        }

        if let Some(metadata) = request.metadata {
            article.metadata = metadata;
        }

        // 更新时间戳
        article.updated_at = Utc::now();
        if content_updated {
            article.last_edited_at = Some(Utc::now());
        }

        // 更新文章
        let thing = Thing::new("article", article_id.to_string());
        let updated_article = self.db.update(thing, article).await?
            .ok_or_else(|| AppError::NotFound("Failed to update article".to_string()))?;

        // 更新标签（如果提供）
        if let Some(tags) = request.tags {
            self.update_article_tags(&updated_article.id, &tags).await?;
        }

        info!("Updated article: {}", article_id);
        Ok(updated_article)
    }

    /// 软删除文章
    pub async fn delete_article(&self, article_id: &str, author_id: &str) -> Result<()> {
        debug!("Deleting article: {} by user: {}", article_id, author_id);

        // 获取文章以验证权限
        let article = self.get_article_by_id(article_id).await?
            .ok_or_else(|| AppError::NotFound("Article not found".to_string()))?;

        if article.author_id != author_id {
            return Err(AppError::Authorization("Only article author can delete this article".to_string()));
        }

        // 软删除
        let query = "UPDATE article SET is_deleted = true, updated_at = time::now() WHERE id = $id";
        self.db.query_with_params(query, json!({
            "id": article_id
        })).await?;

        info!("Deleted article: {}", article_id);
        Ok(())
    }

    /// 根据 ID 获取文章
    pub async fn get_article_by_id(&self, article_id: &str) -> Result<Option<Article>> {
        debug!("Getting article by ID: {}", article_id);

        // 获取纯 ID（不带 table 前缀）
        let pure_id = if article_id.starts_with("article:") {
            &article_id[8..]
        } else {
            article_id
        };

        // 使用反引号包裹 ID 以避免解析问题
        let query = format!("SELECT * FROM article:`{}`", pure_id);
        debug!("Executing query: {}", query);

        let mut response = self.db.query(&query).await?;
        let raw: SurrealValue = response.take(0)?;
        let raw_json = serde_json::to_value(raw)?;
        let list_json = normalize_surreal_json(raw_json);
        let list = match list_json {
            JsonValue::Array(arr) => arr,
            other => vec![other],
        };
        let articles = parse_articles_from_value_list(list)?;
        debug!("Found {} articles", articles.len());
        Ok(articles.into_iter().next())
    }

    /// 根据 slug 获取文章
    pub async fn get_article_by_slug(&self, slug: &str) -> Result<Option<Article>> {
        debug!("Getting article by slug: {}", slug);
        let slug_json = serde_json::to_string(slug)
            .map_err(|e| AppError::Internal(format!("Failed to encode slug: {}", e)))?;
        let query = format!("SELECT * FROM article WHERE slug = {}", slug_json);
        debug!("Executing query: {}", query);
        let mut response = self.db.query(&query).await?;
        let raw: SurrealValue = response.take(0)?;
        let raw_json = serde_json::to_value(raw)?;
        let list_json = normalize_surreal_json(raw_json);
        let list = match list_json {
            JsonValue::Array(arr) => arr,
            other => vec![other],
        };
        let articles = parse_articles_from_value_list(list)?;
        Ok(articles.into_iter().next())
    }

    /// 获取文章完整信息（包含作者、标签、统计等）
    pub async fn get_article_with_details(&self, slug: &str, viewer_user_id: Option<&str>) -> Result<Option<ArticleResponse>> {
        debug!("Getting article with details for slug: {}", slug);

        // 获取文章基础信息
        let article = match self.get_article_by_slug(slug).await? {
            Some(article) => article,
            None => return Ok(None),
        };

        // 获取作者信息
        let author = self.get_article_author(&article.author_id).await?;

        // 获取文章标签
        let tags = self.get_article_tags(&article.id).await?;

        // 获取出版物信息（如果有）
        let publication = match &article.publication_id {
            Some(pub_id) => self.get_article_publication(pub_id).await?,
            None => None,
        };

        // 获取系列信息（如果有）
        let series = match &article.series_id {
            Some(series_id) => self.get_article_series(series_id, &article.id).await?,
            None => None,
        };

        // 获取用户相关信息（如果已登录）
        let (is_bookmarked, is_clapped, user_clap_count) = if let Some(user_id) = viewer_user_id {
            let bookmarked = self.is_article_bookmarked(&article.id, user_id).await?;
            let clapped = self.is_article_clapped(&article.id, user_id).await?;
            let clap_count = self.get_user_clap_count(&article.id, user_id).await?;
            (Some(bookmarked), Some(clapped), Some(clap_count))
        } else {
            (None, None, None)
        };

        let article_response = ArticleResponse {
            id: article.id,
            title: article.title,
            subtitle: article.subtitle,
            slug: article.slug,
            content: article.content,
            content_html: article.content_html,
            excerpt: article.excerpt,
            cover_image_url: article.cover_image_url,
            author,
            publication,
            series,
            status: article.status,
            is_paid_content: article.is_paid_content,
            is_featured: article.is_featured,
            reading_time: article.reading_time,
            word_count: article.word_count,
            view_count: article.view_count,
            clap_count: article.clap_count,
            comment_count: article.comment_count,
            bookmark_count: article.bookmark_count,
            share_count: article.share_count,
            tags,
            seo_title: article.seo_title,
            seo_description: article.seo_description,
            seo_keywords: article.seo_keywords,
            created_at: article.created_at,
            updated_at: article.updated_at,
            published_at: article.published_at,
            is_bookmarked,
            is_clapped,
            user_clap_count,
        };

        Ok(Some(article_response))
    }

    /// 获取文章列表（分页）
    pub async fn get_articles(&self, query: ArticleQuery) -> Result<crate::services::database::PaginatedResult<ArticleListItem>> {
        debug!("Getting articles list with query: {:?}", query);

        let page = query.page.unwrap_or(1);
        let limit = query.limit.unwrap_or(20);
        let offset = (page - 1) * limit;

        // 构建查询条件
        let mut conditions = vec!["is_deleted = false".to_string()];

        // 状态过滤
        if let Some(status) = &query.status {
            conditions.push(format!("status = '{}'", status));
        } else {
            conditions.push("status = 'published'".to_string());
        }

        // 作者过滤
        if let Some(author) = &query.author {
            conditions.push(format!("author_id = $author"));
        }

        // 标签过滤：通过 article_tag 联表查询
        if let Some(_tag) = &query.tag {
            conditions.push("id IN (SELECT VALUE article_id FROM article_tag WHERE tag_id.slug = $tag)".to_string());
        }

        // 出版物过滤
        if let Some(publication) = &query.publication {
            conditions.push(format!("publication_id = $publication"));
        }

        // 精选文章过滤
        if let Some(featured) = query.featured {
            conditions.push(format!("is_featured = {}", featured));
        }

        // 搜索
        if let Some(search_term) = &query.search {
            conditions.push(format!("(title ~ $search OR content ~ $search)"));
        }

        let where_clause = conditions.join(" AND ");

        // 排序
        let (select_fields, order_by) = match query.sort.as_deref() {
            Some("oldest") => ("*", "created_at ASC"),
            Some("popular") => ("*", "clap_count DESC, view_count DESC"),
            Some("trending") => {
                // 在 SELECT 中计算趋势分数
                ("*, (clap_count + comment_count * 2 + view_count * 0.1) as trending_score", "trending_score DESC")
            },
            _ => ("*", "created_at DESC"),
        };

        // 构建查询
        let count_query = format!("SELECT count() AS total FROM article WHERE {}", where_clause);
        let data_query = format!(
            "SELECT {} FROM article WHERE {} ORDER BY {} LIMIT $limit START $offset",
            select_fields, where_clause, order_by
        );

        // 构建参数
        let mut params = json!({
            "limit": limit,
            "offset": offset
        });

        if let Some(author) = &query.author {
            params["author"] = json!(author);
        }
        if let Some(tag) = &query.tag {
            params["tag"] = json!(tag);
        }
        if let Some(publication) = &query.publication {
            params["publication"] = json!(publication);
        }
        if let Some(search_term) = &query.search {
            params["search"] = json!(search_term);
        }

        // 执行查询
        let mut count_response = self.db.query_with_params(&count_query, &params).await?;
        let total = if let Ok(Some(result)) = count_response.take::<Option<JsonValue>>(0) {
            result.get("total").and_then(|v| v.as_i64()).unwrap_or(0) as usize
        } else { 0 };

        let mut data_response = self.db.query_with_params(&data_query, params).await?;
        let raw: SurrealValue = data_response.take(0)?;
        let raw_list = surreal_to_json_list(raw);
        debug!(
            "Raw articles response (count={}): {}",
            raw_list.len(),
            serde_json::to_string(&raw_list).unwrap_or_else(|_| "<unserializable>".to_string())
        );
        let normalized_items: Vec<JsonValue> = raw_list
            .into_iter()
            .map(normalize_surreal_json)
            .collect();
        debug!(
            "Normalized articles response (count={}): {}",
            normalized_items.len(),
            serde_json::to_string(&normalized_items).unwrap_or_else(|_| "<unserializable>".to_string())
        );
        let articles = parse_articles_from_value_list(normalized_items)?;
        
        // 将Article转换为ArticleListItem，并填充作者信息
        let mut article_list_items = Vec::new();
        for article in articles {
            let list_item = self.article_to_list_item(&article).await?;
            article_list_items.push(list_item);
        }

        Ok(crate::services::database::PaginatedResult {
            data: article_list_items,
            total,
            page,
            per_page: limit,
            total_pages: (total + limit - 1) / limit,
        })
    }

    /// 获取用户的文章列表
    pub async fn get_user_articles(&self, user_id: &str, page: usize, limit: usize, include_drafts: bool) -> Result<crate::services::database::PaginatedResult<ArticleListItem>> {
        debug!("Getting articles for user: {} (include_drafts: {})", user_id, include_drafts);

        let mut query = ArticleQuery {
            author: Some(user_id.to_string()),
            page: Some(page),
            limit: Some(limit),
            ..Default::default()
        };

        if include_drafts {
            query.status = None; // 返回所有状态的文章
        }

        self.get_articles(query).await
    }

    /// 增加文章浏览次数
    pub async fn increment_view_count(&self, article_id: &str) -> Result<()> {
        debug!("Incrementing view count for article: {}", article_id);

        let query = "UPDATE article SET view_count += 1, updated_at = time::now() WHERE id = $id";
        self.db.query_with_params(query, json!({
            "id": article_id
        })).await?;

        Ok(())
    }

    /// 增加文章鼓掌数
    pub async fn increment_clap_count(&self, article_id: &str, count: u32) -> Result<()> {
        debug!("Incrementing clap count for article: {} by {}", article_id, count);

        let query = "UPDATE article SET clap_count += $count, updated_at = time::now() WHERE id = $id";
        self.db.query_with_params(query, json!({
            "id": article_id,
            "count": count
        })).await?;

        Ok(())
    }

    /// 更新文章评论数
    pub async fn update_comment_count(&self, article_id: &str) -> Result<()> {
        debug!("Updating comment count for article: {}", article_id);

        let query = r#"
            LET $count = count((SELECT * FROM comment WHERE article_id = $id AND is_deleted = false));
            UPDATE article SET comment_count = $count, updated_at = time::now() WHERE id = $id;
        "#;
        
        self.db.query_with_params(query, json!({
            "id": article_id
        })).await?;

        Ok(())
    }

    /// 生成唯一的 slug
    async fn generate_unique_slug(&self, title: &str) -> Result<String> {
        let base_slug = slug::generate_slug(title);
        let mut slug = base_slug.clone();
        let mut counter = 1;

        while self.slug_exists(&slug).await? {
            slug = format!("{}-{}", base_slug, counter);
            counter += 1;
            
            if counter > 100 {
                return Err(AppError::Internal("Failed to generate unique slug".to_string()));
            }
        }

        Ok(slug)
    }

    async fn slug_exists(&self, slug: &str) -> Result<bool> {
        let query = "SELECT VALUE count() FROM article WHERE slug = $slug";
        let mut response = self.db.query_with_params(query, json!({ "slug": slug })).await?;
        let counts: Vec<i64> = response.take(0)?;
        Ok(counts.into_iter().next().unwrap_or(0) > 0)
    }

    /// 为文章附加标签
    async fn attach_tags_to_article(&self, article_id: &str, tags: &[String]) -> Result<()> {
        debug!("Attaching {} tags to article: {}", tags.len(), article_id);

        let normalized_article_id = normalize_surreal_id(article_id);

        // 清理现有标签（规范为 record 类型进行匹配）
        let clear_query = r#"
            DELETE article_tag 
            WHERE article_id = type::record("article", $aid)
        "#;
        self.db
            .query_with_params(clear_query, json!({ "aid": normalized_article_id }))
            .await?;

        // 添加新标签
        for tag_name in tags {
            // 获取或创建标签
            let tag_id = self.get_or_create_tag(tag_name).await?;
            let normalized_tag_id = normalize_surreal_id(&tag_id);

            // 创建关联（确保以 record 类型写入）
            let create_query = r#"
                CREATE article_tag SET 
                    article_id = type::record("article", $aid),
                    tag_id = type::record("tag", $tid)
            "#;
            self.db
                .query_with_params(create_query, json!({
                    "aid": normalized_article_id,
                    "tid": normalized_tag_id
                }))
                .await?;

            // 更新该标签的文章计数
            let count_query = r#"
                SELECT VALUE count() FROM article_tag 
                WHERE tag_id = type::record("tag", $tid)
            "#;
            let mut resp = self
                .db
                .query_with_params(count_query, json!({ "tid": normalized_tag_id }))
                .await?;
            let counts: Vec<i64> = resp.take(0)?;
            let count = counts.into_iter().next().unwrap_or(0);

            let update_count = r#"
                UPDATE type::record("tag", $tid) SET article_count = $count
            "#;
            self.db
                .query_with_params(update_count, json!({
                    "tid": normalized_tag_id,
                    "count": count
                }))
                .await?;
        }

        // 更新文章的标签字段
        let update_query = "UPDATE article SET tags = $tags WHERE id = $id";
        self.db.query_with_params(update_query, json!({
            "id": article_id,
            "tags": tags
        })).await?;

        Ok(())
    }

    /// 更新文章标签
    async fn update_article_tags(&self, article_id: &str, tags: &[String]) -> Result<()> {
        self.attach_tags_to_article(article_id, tags).await
    }

    /// 获取或创建标签
    async fn get_or_create_tag(&self, tag_name: &str) -> Result<String> {
        let slug = slug::generate_slug(tag_name);
        debug!("Getting or creating tag: {} (slug={})", tag_name, slug);

        // Check if tag exists using VALUE query which returns bare values
        let check = self.db.query_with_params(
            "SELECT VALUE type::string(id) FROM tag WHERE slug = $slug LIMIT 1",
            json!({ "slug": &slug }),
        ).await;

        if let Ok(mut r) = check {
            let ids: Vec<JsonValue> = r.take(0).unwrap_or_default();
            if let Some(id_val) = ids.into_iter().next() {
                let id_str = match &id_val {
                    JsonValue::String(s) => s.clone(),
                    other => other.to_string().trim_matches('"').to_string(),
                };
                if !id_str.is_empty() && id_str != "null" {
                    debug!("Found existing tag: {}", id_str);
                    return Ok(id_str);
                }
            }
        }

        // Create new tag
        let tag_uuid = Uuid::new_v4().to_string();
        let create_query = format!(
            "CREATE tag:`{uuid}` CONTENT {{ name: $name, slug: $slug, follower_count: 0, article_count: 0, is_featured: false, created_at: time::now(), updated_at: time::now() }} RETURN VALUE type::string(id)",
            uuid = tag_uuid
        );

        match self.db.query_with_params(&create_query, json!({ "name": tag_name, "slug": &slug })).await {
            Ok(mut result) => {
                let ids: Vec<JsonValue> = result.take(0).unwrap_or_default();
                let id_str = ids.into_iter().next()
                    .and_then(|v| match v { JsonValue::String(s) => Some(s), _ => None })
                    .unwrap_or_else(|| format!("tag:{}", tag_uuid));
                debug!("Created tag: {}", id_str);
                Ok(id_str)
            }
            Err(e) => {
                // Race condition: another request just created it — fetch and return
                warn!("Tag create race for slug='{}': {}, fetching existing", slug, e);
                let mut r2 = self.db.query_with_params(
                    "SELECT VALUE type::string(id) FROM tag WHERE slug = $slug LIMIT 1",
                    json!({ "slug": &slug }),
                ).await?;
                let ids: Vec<JsonValue> = r2.take(0).unwrap_or_default();
                ids.into_iter().next()
                    .and_then(|v| match v { JsonValue::String(s) if !s.is_empty() => Some(s), _ => None })
                    .ok_or_else(|| AppError::Internal(format!("Failed to get or create tag '{}'", tag_name)))
            }
        }
    }

    /// 发布文章
    pub async fn publish_article(&self, article_id: &str, author_id: &str) -> Result<Article> {
        debug!("Publishing article: {} by user: {}", article_id, author_id);
        
        // 获取文章
        let mut article = self.get_article_by_id(article_id).await?
            .ok_or_else(|| AppError::NotFound("Article not found".to_string()))?;
        
        // 检查权限
        if article.author_id != author_id {
            return Err(AppError::Authorization("Only article author can publish this article".to_string()));
        }
        
        // 检查是否已发布
        if article.status == ArticleStatus::Published {
            return Err(AppError::BadRequest("Article is already published".to_string()));
        }
        
        // 使用 UPDATE 查询而不是对象更新，避免 ID 格式问题
        let id_without_prefix = if article_id.starts_with("article:") {
            &article_id[8..]
        } else {
            article_id
        };
        
        let update_query = format!(
            "UPDATE article:`{}` SET status = $status, published_at = time::now(), updated_at = time::now() RETURN *",
            id_without_prefix
        );
        
        let mut response = self.db.query_with_params(&update_query, json!({
            "status": "published"
        })).await?;

        let raw: SurrealValue = response.take(0)?;
        let raw_json = serde_json::to_value(raw)?;
        let list_json = normalize_surreal_json(raw_json);
        let updated_articles: Vec<Article> = serde_json::from_value(list_json)?;
        let updated_article = updated_articles.into_iter().next()
            .ok_or_else(|| AppError::NotFound("Failed to publish article".to_string()))?;
        
        info!("Published article: {}", article_id);
        Ok(updated_article)
    }
    
    /// 取消发布文章
    pub async fn unpublish_article(&self, article_id: &str, author_id: &str) -> Result<Article> {
        debug!("Unpublishing article: {} by user: {}", article_id, author_id);
        
        // 获取文章
        let mut article = self.get_article_by_id(article_id).await?
            .ok_or_else(|| AppError::NotFound("Article not found".to_string()))?;
        
        // 检查权限
        if article.author_id != author_id {
            return Err(AppError::Authorization("Only article author can unpublish this article".to_string()));
        }
        
        // 检查是否已是草稿
        if article.status == ArticleStatus::Draft {
            return Err(AppError::BadRequest("Article is already in draft status".to_string()));
        }
        
        // 使用 UPDATE 查询而不是对象更新，避免 ID 格式问题
        let id_without_prefix = if article_id.starts_with("article:") {
            &article_id[8..]
        } else {
            article_id
        };
        
        let update_query = format!(
            "UPDATE article:`{}` SET status = $status, updated_at = time::now() RETURN *",
            id_without_prefix
        );
        
        let mut response = self.db.query_with_params(&update_query, json!({
            "status": "draft"
        })).await?;

        let raw: SurrealValue = response.take(0)?;
        let raw_json = serde_json::to_value(raw)?;
        let list_json = normalize_surreal_json(raw_json);
        let updated_articles: Vec<Article> = serde_json::from_value(list_json)?;
        let updated_article = updated_articles.into_iter().next()
            .ok_or_else(|| AppError::NotFound("Failed to unpublish article".to_string()))?;
        
        info!("Unpublished article: {}", article_id);
        Ok(updated_article)
    }

    /// 聚合每日统计
    pub async fn aggregate_daily_stats(&self) -> Result<()> {
        debug!("Aggregating daily article stats");

        // 使用更简单的方法来避免复杂的字段名
        let today = Utc::now().date_naive().and_hms_opt(0, 0, 0).unwrap();
        let tomorrow = today + chrono::Duration::days(1);
        let today_rfc3339 = chrono::DateTime::<Utc>::from_naive_utc_and_offset(today, Utc).to_rfc3339();
        let tomorrow_rfc3339 =
            chrono::DateTime::<Utc>::from_naive_utc_and_offset(tomorrow, Utc).to_rfc3339();
        
        // 先获取统计数据（在应用层聚合，避免 Surreal 函数兼容性问题）
        let stats_query = r#"
            SELECT view_count, clap_count, comment_count, reading_time
            FROM article
            WHERE created_at >= $today 
            AND created_at < $tomorrow
        "#;
        
        let mut response = self.db.query_with_params(stats_query, json!({
            "today": today_rfc3339,
            "tomorrow": tomorrow_rfc3339
        })).await?;
        let raw_json: JsonValue = response.take(0)?;
        let list_json = normalize_surreal_json(raw_json);
        let rows: Vec<JsonValue> = serde_json::from_value(list_json)?;

        let total_articles = rows.len() as i64;
        let mut total_views = 0i64;
        let mut total_claps = 0i64;
        let mut total_comments = 0i64;
        let mut total_reading_time = 0f64;

        for row in rows.iter() {
            total_views += row.get("view_count").and_then(|v| v.as_i64()).unwrap_or(0);
            total_claps += row.get("clap_count").and_then(|v| v.as_i64()).unwrap_or(0);
            total_comments += row.get("comment_count").and_then(|v| v.as_i64()).unwrap_or(0);
            total_reading_time += row.get("reading_time").and_then(|v| v.as_f64()).unwrap_or(0.0);
        }

        let avg_reading_time = if total_articles > 0 {
            total_reading_time / total_articles as f64
        } else {
            0.0
        };

        if total_articles >= 0 {
            // 创建或更新统计记录（SurrealDB 3: 使用 type::thing 避免 record-id 参数被解析为序列）
            let upsert_query = r#"
                UPDATE type::record($record_id) MERGE $stats
            "#;
            let today_id = today.date().to_string();
            let record_id = format!("daily_article_stats:{}", today_id);

            let stats_data = json!({
                "date": today_id,
                "total_articles": total_articles,
                "total_views": total_views,
                "total_claps": total_claps,
                "total_comments": total_comments,
                "avg_reading_time": avg_reading_time,
                "updated_at": Utc::now().to_rfc3339()
            });
            
            self.db.query_with_params(upsert_query, json!({
                "record_id": record_id,
                "stats": stats_data
            })).await?;
        }
        
        Ok(())
    }

    /// 获取文章作者信息
    async fn get_article_author(&self, author_id: &str) -> Result<AuthorInfo> {
        debug!("Getting author info for: {}", author_id);

        let query = r#"
            SELECT id, username, display_name, avatar_url, is_verified 
            FROM user_profile 
            WHERE user_id = $author_id
            LIMIT 1
        "#;

        let mut response = self.db.query_with_params(query, json!({
            "author_id": author_id
        })).await?;

        // 直接获取原始数据
        let raw: SurrealValue = response.take(0)?;
        let results = surreal_to_json_list(raw);
        let author_data = results.into_iter().next()
            .ok_or_else(|| AppError::NotFound(format!("Author {} not found", author_id)))?;

        let avatar_url = match author_data.get("avatar_url") {
            Some(JsonValue::String(s)) => {
                let trimmed = s.trim();
                if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            }
            _ => None,
        };

        // 手动构造 AuthorInfo
        Ok(AuthorInfo {
            id: author_data.get("id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            username: author_data.get("username")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            display_name: author_data.get("display_name")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            avatar_url,
            is_verified: author_data.get("is_verified")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        })
    }

    /// 获取文章标签
    async fn get_article_tags(&self, article_id: &str) -> Result<Vec<TagInfo>> {
        debug!("Getting tags for article: {}", article_id);

        // Dereference tag_id directly — returns tag fields inline
        let normalized = normalize_surreal_id(article_id);
        let query = r#"
            SELECT tag_id.id AS id, tag_id.name AS name, tag_id.slug AS slug
            FROM article_tag
            WHERE article_id = type::record("article", $aid)
        "#;

        let mut response = self.db.query_with_params(query, json!({
            "aid": normalized
        })).await?;

        let raw: SurrealValue = response.take(0)?;
        let raw_json = serde_json::to_value(raw)?;
        let list_json = normalize_surreal_json(raw_json);
        let rows = match list_json {
            JsonValue::Array(arr) => arr,
            other if other.is_null() => vec![],
            other => vec![other],
        };

        let mut tags = Vec::new();
        for row in rows {
            let row = normalize_surreal_json(row);
            if let JsonValue::Object(map) = &row {
                let id = map.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let name = map.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let slug = map.get("slug").and_then(|v| v.as_str()).unwrap_or("").to_string();
                if !name.is_empty() {
                    tags.push(TagInfo { id, name, slug });
                }
            }
        }

        tags.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(tags)
    }

    /// 获取文章出版物信息
    async fn get_article_publication(&self, publication_id: &str) -> Result<Option<PublicationInfo>> {
        debug!("Getting publication info for: {}", publication_id);

        let query = r#"
            SELECT id, name, slug, logo_url 
            FROM publication 
            WHERE id = $publication_id
        "#;

        let mut response = self.db.query_with_params(query, json!({
            "publication_id": publication_id
        })).await?;

        let publications: Vec<PublicationInfo> = response.take(0)?;
        Ok(publications.into_iter().next())
    }

    /// 获取文章系列信息
    async fn get_article_series(&self, series_id: &str, article_id: &str) -> Result<Option<SeriesInfo>> {
        debug!("Getting series info for: {}", series_id);

        // First get the series
        let series_query = r#"
            SELECT id, title, slug FROM series WHERE id = $series_id LIMIT 1
        "#;

        let mut response = self.db.query_with_params(series_query, json!({
            "series_id": series_id
        })).await?;

        let series_data: Vec<JsonValue> = response.take(0)?;
        if let Some(series) = series_data.into_iter().next() {
            // Then get the order from series_article
            let order_query = r#"
                SELECT `order` FROM series_article 
                WHERE series_id = $series_id AND article_id = $article_id
                LIMIT 1
            "#;
            
            let mut order_response = self.db.query_with_params(order_query, json!({
                "series_id": series_id,
                "article_id": article_id
            })).await?;
            
            let order_data: Vec<JsonValue> = order_response.take(0)?;
            let order = order_data.into_iter().next()
                .and_then(|v| v.get("order").and_then(|o| o.as_i64()))
                .unwrap_or(0) as i32;
            
            Ok(Some(SeriesInfo {
                id: series.get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                title: series.get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                slug: series.get("slug")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                order,
            }))
        } else {
            Ok(None)
        }
    }

    /// 检查用户是否收藏了文章
    async fn is_article_bookmarked(&self, article_id: &str, user_id: &str) -> Result<bool> {
        let query = r#"
            SELECT count() as count 
            FROM bookmark 
            WHERE article_id = $article_id AND user_id = $user_id AND is_deleted = false
        "#;

        let mut response = self.db.query_with_params(query, json!({
            "article_id": article_id,
            "user_id": user_id
        })).await?;

        let result: Vec<serde_json::Value> = response.take(0)?;
        let count = result.first()
            .and_then(|v| v.get("count"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        Ok(count > 0)
    }

    /// 检查用户是否点赞了文章
    async fn is_article_clapped(&self, article_id: &str, user_id: &str) -> Result<bool> {
        let query = r#"
            SELECT count() as count 
            FROM clap 
            WHERE article_id = $article_id AND user_id = $user_id
        "#;

        let mut response = self.db.query_with_params(query, json!({
            "article_id": article_id,
            "user_id": user_id
        })).await?;

        let result: Vec<serde_json::Value> = response.take(0)?;
        let count = result.first()
            .and_then(|v| v.get("count"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        Ok(count > 0)
    }

    /// 获取用户对文章的点赞次数
    async fn get_user_clap_count(&self, article_id: &str, user_id: &str) -> Result<i32> {
        let query = r#"
            SELECT count 
            FROM clap 
            WHERE article_id = $article_id AND user_id = $user_id
            LIMIT 1
        "#;

        let mut response = self.db.query_with_params(query, json!({
            "article_id": article_id,
            "user_id": user_id
        })).await?;

        let result: Vec<serde_json::Value> = response.take(0)?;
        let count = result.first()
            .and_then(|v| v.get("count"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as i32;

        Ok(count)
    }

    /// 为文章添加点赞
    pub async fn clap_article(&self, article_id: &str, user_id: &str, count: i32) -> Result<crate::models::clap::ClapResponse> {
        debug!("User {} clapping article {} with count {}", user_id, article_id, count);

        // 验证文章存在且已发布
        let article = self.get_article_by_id(article_id).await
            .map_err(|e| {
                error!("Failed to get article by id {}: {:?}", article_id, e);
                e
            })?
            .ok_or_else(|| {
                error!("Article not found: {}", article_id);
                AppError::NotFound("Article not found".to_string())
            })?;

        if article.status != ArticleStatus::Published {
            return Err(AppError::forbidden("Cannot clap unpublished articles"));
        }

        // 获取用户现有的点赞
        let query = format!(r#"
            SELECT meta::tb(id) as tb, meta::id(id) as id_val, count FROM clap 
            WHERE user_id = $user_id 
            AND article_id = article:`{}`
        "#, article_id);
        
        debug!("Querying existing claps with user_id: {} and article_id: {}", user_id, article_id);
        
        let mut response = self.db
            .query_with_params(&query, json!({
                "user_id": user_id
            }))
            .await
            .map_err(|e| {
                error!("Failed to query existing claps: {:?}", e);
                e
            })?;
        let clap_data: Vec<JsonValue> = response.take(0)?;
        let existing_clap = clap_data.into_iter().next();

        let user_clap_count = if let Some(clap_value) = existing_clap {
            // 获取现有点赞数
            let current_count = clap_value.get("count")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as i32;
            
            // 检查总数是否超过50
            let new_total = current_count + count;
            if new_total > 50 {
                return Err(AppError::BadRequest(
                    format!("Maximum claps per article is 50. You have {} claps already.", current_count)
                ));
            }

            // 获取点赞ID - 使用meta函数返回的值
            let tb = clap_value.get("tb")
                .and_then(|v| v.as_str())
                .ok_or_else(|| AppError::internal("Missing table name"))?;
            let id_val = clap_value.get("id_val")
                .and_then(|v| v.as_str())
                .ok_or_else(|| AppError::internal("Missing ID value"))?;
            
            let clap_id = format!("{}:{}", tb, id_val);
            debug!("Updating clap with ID: {}", clap_id);

            // 更新现有点赞 - 使用反引号包裹ID
            let update_query = format!(
                "UPDATE clap:`{}` SET count = $count, updated_at = time::now() RETURN count",
                id_val
            );
            
            let mut update_response = self.db.query_with_params(&update_query, json!({
                "count": new_total
            })).await?;
            
            let result: Vec<JsonValue> = update_response.take(0)?;
            result.into_iter().next()
                .and_then(|v| v.get("count").and_then(|c| c.as_i64()))
                .unwrap_or(new_total as i64) as i32
        } else {
            // 创建新点赞
            if count > 50 {
                return Err(AppError::BadRequest("Maximum claps per article is 50".to_string()));
            }

            // 使用 SQL 创建点赞记录，article_id 使用 record 类型
            let clap_id = Uuid::new_v4().to_string();
            let create_query = format!(r#"
                CREATE clap:`{}` CONTENT {{
                    user_id: $user_id,
                    article_id: article:`{}`,
                    count: $count,
                    created_at: time::now(),
                    updated_at: time::now()
                }}
            "#, clap_id, article_id);
            
            let mut create_response = self.db.query_with_params(&create_query, json!({
                "user_id": user_id,
                "count": count
            })).await?;
            
            // 检查创建是否成功
            let created_results: Vec<JsonValue> = create_response.take(0)?;
            debug!("Created clap results: {:?}", created_results);
            
            count
        };

        // 更新文章总点赞数
        debug!("Updating article clap count for article_id: {}", article_id);
        self.update_article_clap_count(article_id).await?;

        // 获取文章最新的总点赞数
        debug!("Getting total claps for article_id: {}", article_id);
        let total_claps = self.get_article_total_claps(article_id).await?;

        Ok(crate::models::clap::ClapResponse {
            user_clap_count,
            total_claps,
        })
    }

    /// 更新文章的总点赞数
    async fn update_article_clap_count(&self, article_id: &str) -> Result<()> {
        // 获取所有点赞记录的count值
        let count_query = format!(
            "SELECT count FROM clap WHERE article_id = article:`{}`",
            article_id
        );
        
        debug!("Getting all clap counts for article: {}", article_id);
        
        let mut count_response = self.db.query(&count_query).await?;
        let clap_records: Vec<JsonValue> = count_response.take(0)?;
        
        // 在应用层计算总和
        let total_claps: i64 = clap_records.iter()
            .filter_map(|v| v.get("count"))
            .filter_map(|v| v.as_i64())
            .sum();
        
        debug!("Total claps calculated for article {}: {}", article_id, total_claps);
        
        // 更新文章的点赞数
        let update_query = format!(
            "UPDATE article:`{}` SET clap_count = {}",
            article_id, total_claps
        );
        
        debug!("Updating article clap_count with query: {}", update_query);
        
        self.db.query(&update_query).await?;
        
        info!("Successfully updated article {} clap_count to {}", article_id, total_claps);

        Ok(())
    }

    /// 获取文章的总点赞数
    async fn get_article_total_claps(&self, article_id: &str) -> Result<i64> {
        let query = format!("SELECT clap_count FROM article:`{}`", article_id);

        let mut response = self.db.query(&query).await?;

        let result: Vec<serde_json::Value> = response.take(0)?;
        let count = result.first()
            .and_then(|v| v.get("clap_count"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        Ok(count)
    }

    /// 获取出版物的文章列表
    pub async fn get_articles_by_publication(
        &self, 
        publication_id: &str, 
        page: usize, 
        per_page: usize, 
        tag: Option<&str>,
        search: Option<&str>
    ) -> Result<Vec<ArticleListItem>> {
        debug!("Getting articles for publication: {}", publication_id);
        
        let offset = (page - 1) * per_page;
        
        // 构建查询条件
        let mut conditions = vec![
            "publication_id = $publication_id".to_string(),
            "status = 'published'".to_string(),
            "is_deleted = false".to_string(),
        ];
        
        // 添加标签过滤
        if let Some(tag) = tag {
            conditions.push(format!("$tag IN tags"));
        }
        
        // 添加搜索过滤
        if let Some(search_term) = search {
            conditions.push(format!("(title ~ $search OR content ~ $search)"));
        }
        
        let where_clause = conditions.join(" AND ");
        
        let query = format!(r#"
            SELECT 
                id, title, subtitle, slug, excerpt, cover_image_url,
                author_id, publication_id, reading_time, 
                view_count, clap_count, comment_count,
                created_at, published_at
            FROM article 
            WHERE {}
            ORDER BY published_at DESC
            LIMIT $limit START $offset
        "#, where_clause);
        
        let mut params = json!({
            "publication_id": publication_id,
            "limit": per_page,
            "offset": offset
        });
        
        if let Some(tag) = tag {
            params["tag"] = json!(tag);
        }
        
        if let Some(search_term) = search {
            params["search"] = json!(search_term);
        }
        
        let mut response = self.db.query_with_params(&query, params).await?;
        let raw: SurrealValue = response.take(0)?;
        let raw_list = surreal_to_json_list(raw);
        let articles: Vec<ArticleListItem> = parse_from_json_list(raw_list);
        
        Ok(articles)
    }
    
    /// 统计出版物的文章总数
    pub async fn count_articles_by_publication(
        &self, 
        publication_id: &str,
        tag: Option<&str>,
        search: Option<&str>
    ) -> Result<usize> {
        debug!("Counting articles for publication: {}", publication_id);
        
        // 构建查询条件
        let mut conditions = vec![
            "publication_id = $publication_id".to_string(),
            "status = 'published'".to_string(),
            "is_deleted = false".to_string(),
        ];
        
        // 添加标签过滤
        if let Some(tag) = tag {
            conditions.push(format!("$tag IN tags"));
        }
        
        // 添加搜索过滤
        if let Some(search_term) = search {
            conditions.push(format!("(title ~ $search OR content ~ $search)"));
        }
        
        let where_clause = conditions.join(" AND ");
        
        let query = format!(r#"
            SELECT count() as total FROM article 
            WHERE {}
        "#, where_clause);
        
        let mut params = json!({
            "publication_id": publication_id
        });
        
        if let Some(tag) = tag {
            params["tag"] = json!(tag);
        }
        
        if let Some(search_term) = search {
            params["search"] = json!(search_term);
        }
        
        let mut response = self.db.query_with_params(&query, params).await?;
        
        let result: Vec<serde_json::Value> = response.take(0)?;
        let count = result.first()
            .and_then(|v| v.get("total"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;
        
        Ok(count)
    }

    /// 获取出版物中特定slug的文章
    pub async fn get_article_by_slug_in_publication(
        &self,
        publication_id: &str,
        slug: &str,
        viewer_user_id: Option<&str>
    ) -> Result<Option<ArticleResponse>> {
        debug!("Getting article by slug {} in publication {}", slug, publication_id);
        
        // 获取文章基础信息并检查是否属于该出版物
        let article = match self.get_article_by_slug(slug).await? {
            Some(article) => article,
            None => return Ok(None),
        };
        
        // 检查文章是否属于该出版物
        if article.publication_id.as_deref() != Some(publication_id) {
            return Ok(None);
        }
        
        // 获取完整的文章信息
        self.get_article_with_details(slug, viewer_user_id).await
    }
    
    /// 获取出版物中的相关文章
    pub async fn get_related_articles_in_publication(
        &self,
        publication_id: &str,
        article_id: &str,
        limit: usize
    ) -> Result<Vec<ArticleListItem>> {
        debug!("Getting related articles for {} in publication {}", article_id, publication_id);
        
        // 获取当前文章的标签
        let tags = self.get_article_tags(article_id).await?;
        let tag_ids: Vec<String> = tags.iter().map(|t| t.id.clone()).collect();
        
        if tag_ids.is_empty() {
            // 如果没有标签，返回该出版物最新的文章
            return self.get_articles_by_publication(publication_id, 1, limit, None, None).await;
        }
        
        // 基于标签查找相关文章
        let query = r#"
            SELECT DISTINCT
                a.id, a.title, a.subtitle, a.slug, a.excerpt, a.cover_image_url,
                a.author_id, a.publication_id, a.reading_time, 
                a.view_count, a.clap_count, a.comment_count,
                a.created_at, a.published_at
            FROM article a
            JOIN article_tag at ON a.id = at.article_id
            WHERE a.publication_id = $publication_id
                AND a.id != $article_id
                AND at.tag_id IN $tag_ids
                AND a.status = 'published'
                AND a.is_deleted = false
            ORDER BY a.published_at DESC
            LIMIT $limit
        "#;
        
        let mut response = self.db.query_with_params(query, json!({
            "publication_id": publication_id,
            "article_id": article_id,
            "tag_ids": tag_ids,
            "limit": limit
        })).await?;

        let raw: SurrealValue = response.take(0)?;
        let raw_list = surreal_to_json_list(raw);
        let articles: Vec<ArticleListItem> = parse_from_json_list(raw_list);
        Ok(articles)
    }
    
    /// 获取出版物中特定用户的文章数量
    pub async fn count_articles_by_user_in_publication(
        &self,
        publication_id: &str,
        user_id: &str
    ) -> Result<usize> {
        debug!("Counting articles by user {} in publication {}", user_id, publication_id);
        
        let query = r#"
            SELECT count() as total 
            FROM article 
            WHERE publication_id = $publication_id 
                AND author_id = $user_id
                AND status = 'published' 
                AND is_deleted = false
        "#;
        
        let mut response = self.db.query_with_params(query, json!({
            "publication_id": publication_id,
            "user_id": user_id
        })).await?;
        
        let result: Vec<serde_json::Value> = response.take(0)?;
        let count = result.first()
            .and_then(|v| v.get("total"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;
        
        Ok(count)
    }
    
    /// 获取出版物的总浏览量
    pub async fn get_total_views_by_publication(&self, publication_id: &str) -> Result<usize> {
        debug!("Getting total views for publication {}", publication_id);
        let query = r#"
            SELECT view_count
            FROM article
            WHERE publication_id = $publication_id
                AND status = 'published'
                AND is_deleted = false
        "#;

        let mut response = self.db.query_with_params(query, json!({
            "publication_id": publication_id
        })).await?;

        let raw: SurrealValue = response.take(0)?;
        let values = surreal_to_json_list(raw);
        let total_views: i64 = values
            .iter()
            .filter_map(|v| v.get("view_count"))
            .filter_map(|v| v.as_i64())
            .sum();

        Ok(total_views.max(0) as usize)
    }
    
    /// Helper method to convert article data to ArticleListItem
    async fn article_to_list_item(&self, article: &Article) -> Result<ArticleListItem> {
        // Get author info
        let author_query = r#"
            SELECT id, username, display_name, avatar_url, is_verified
            FROM user_profile
            WHERE user_id = $author_id
        "#;
        
        let mut author_response = self.db.query_with_params(author_query, json!({
            "author_id": &article.author_id
        })).await?;
        
        let author_raw: SurrealValue = author_response.take(0)?;
        let author_data: Vec<JsonValue> = surreal_to_json_list(author_raw)
            .into_iter()
            .map(normalize_surreal_json)
            .collect();
        let author_info = if let Some(author) = author_data.first() {
            let avatar_url = match author.get("avatar_url") {
                Some(JsonValue::String(s)) => {
                    let trimmed = s.trim();
                    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                }
                _ => None,
            };
            AuthorInfo {
                id: author["id"].as_str().unwrap_or("").to_string(),
                username: author["username"].as_str().unwrap_or("").to_string(),
                display_name: author["display_name"].as_str().unwrap_or("").to_string(),
                avatar_url,
                is_verified: author["is_verified"].as_bool().unwrap_or(false),
            }
        } else {
            AuthorInfo {
                id: article.author_id.clone(),
                username: "unknown".to_string(),
                display_name: "Unknown Author".to_string(),
                avatar_url: None,
                is_verified: false,
            }
        };
        
        // Get publication info if exists
        let publication_info = if let Some(pub_id) = &article.publication_id {
            let pub_query = r#"
                SELECT id, name, slug, logo_url
                FROM publication
                WHERE id = $publication_id
            "#;
            
            let mut pub_response = self.db.query_with_params(pub_query, json!({
                "publication_id": pub_id
            })).await?;

            let pub_raw: SurrealValue = pub_response.take(0)?;
            let pub_data: Vec<JsonValue> = surreal_to_json_list(pub_raw)
                .into_iter()
                .map(normalize_surreal_json)
                .collect();
            pub_data.first().map(|p| PublicationInfo {
                id: p["id"].as_str().unwrap_or("").to_string(),
                name: p["name"].as_str().unwrap_or("").to_string(),
                slug: p["slug"].as_str().unwrap_or("").to_string(),
                logo_url: p["logo_url"].as_str().map(String::from),
            })
        } else {
            None
        };
        
        // Get tags via record dereference
        let normalized_aid = normalize_surreal_id(&article.id);
        let tag_query = r#"
            SELECT tag_id.id AS id, tag_id.name AS name, tag_id.slug AS slug
            FROM article_tag
            WHERE article_id = type::record("article", $aid)
        "#;
        let mut tags: Vec<TagInfo> = Vec::new();
        if let Ok(mut tag_resp) = self.db.query_with_params(tag_query, json!({ "aid": normalized_aid })).await {
            if let Ok(raw) = tag_resp.take::<SurrealValue>(0) {
                let raw_json = serde_json::to_value(raw).unwrap_or(JsonValue::Null);
                let list_json = normalize_surreal_json(raw_json);
                let rows = match list_json {
                    JsonValue::Array(arr) => arr,
                    other if other.is_null() => vec![],
                    other => vec![other],
                };
                for row in rows {
                    let row = normalize_surreal_json(row);
                    if let JsonValue::Object(map) = &row {
                        let id = map.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let name = map.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let slug = map.get("slug").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        if !name.is_empty() {
                            tags.push(TagInfo { id, name, slug });
                        }
                    }
                }
            }
        }
        
        Ok(ArticleListItem {
            id: article.id.clone(),
            title: article.title.clone(),
            subtitle: article.subtitle.clone(),
            slug: article.slug.clone(),
            excerpt: article.excerpt.clone(),
            cover_image_url: article.cover_image_url.clone(),
            author: author_info,
            publication: publication_info,
            status: article.status.clone(),
            is_paid_content: article.is_paid_content,
            is_featured: article.is_featured,
            reading_time: article.reading_time,
            view_count: article.view_count,
            clap_count: article.clap_count,
            comment_count: article.comment_count,
            tags,
            created_at: article.created_at,
            published_at: article.published_at,
        })
    }
}
