use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use validator::Validate;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Article {
    #[serde(with = "crate::utils::serde_helpers::thing_id")]
    pub id: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub slug: String,
    pub content: String,
    pub content_html: String,
    pub excerpt: Option<String>,
    pub cover_image_url: Option<String>,
    #[serde(with = "crate::utils::serde_helpers::thing_id")]
    pub author_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub series_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub series_order: Option<i32>,
    pub status: ArticleStatus,
    pub is_paid_content: bool,
    pub is_featured: bool,
    pub reading_time: i32, // 分钟
    pub word_count: i32,
    pub view_count: i64,
    pub clap_count: i64,
    pub comment_count: i64,
    pub bookmark_count: i64,
    pub share_count: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seo_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seo_description: Option<String>,
    pub seo_keywords: Vec<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_edited_at: Option<DateTime<Utc>>,
    pub is_deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ArticleStatus {
    Draft,
    Published,
    Unlisted,
    Archived,
}

impl Default for ArticleStatus {
    fn default() -> Self {
        Self::Draft
    }
}

impl ArticleStatus {
    pub fn can_be_viewed_by_public(&self) -> bool {
        matches!(self, Self::Published | Self::Unlisted)
    }
}

#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct CreateArticleRequest {
    #[validate(length(min = 1, max = 150))]
    pub title: String,
    
    #[validate(length(max = 200))]
    pub subtitle: Option<String>,
    
    #[validate(length(max = 50000))] // 从配置读取
    pub content: String,
    
    #[validate(length(max = 300))]
    pub excerpt: Option<String>,
    
    #[validate(url)]
    pub cover_image_url: Option<String>,
    
    pub publication_id: Option<String>,
    pub series_id: Option<String>,
    pub series_order: Option<i32>,
    pub is_paid_content: Option<bool>,
    pub tags: Option<Vec<String>>,
    
    #[validate(length(max = 60))]
    pub seo_title: Option<String>,
    
    #[validate(length(max = 160))]
    pub seo_description: Option<String>,
    
    pub seo_keywords: Option<Vec<String>>,
    pub save_as_draft: Option<bool>,
    pub status: Option<ArticleStatus>,
}

#[derive(Debug, Serialize, Deserialize, Validate)]
pub struct UpdateArticleRequest {
    #[validate(length(min = 1, max = 150))]
    pub title: Option<String>,
    
    #[validate(length(max = 200))]
    pub subtitle: Option<String>,
    
    #[validate(length(max = 50000))]
    pub content: Option<String>,
    
    #[validate(length(max = 300))]
    pub excerpt: Option<String>,
    
    #[validate(url)]
    pub cover_image_url: Option<String>,
    
    pub publication_id: Option<String>,
    pub series_id: Option<String>,
    pub series_order: Option<i32>,
    pub is_paid_content: Option<bool>,
    pub tags: Option<Vec<String>>,
    
    #[validate(length(max = 60))]
    pub seo_title: Option<String>,
    
    #[validate(length(max = 160))]
    pub seo_description: Option<String>,
    
    pub seo_keywords: Option<Vec<String>>,
    pub status: Option<ArticleStatus>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArticleResponse {
    pub id: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub slug: String,
    pub content: String,
    pub content_html: String,
    pub excerpt: Option<String>,
    pub cover_image_url: Option<String>,
    pub author: AuthorInfo,
    pub publication: Option<PublicationInfo>,
    pub series: Option<SeriesInfo>,
    pub status: ArticleStatus,
    pub is_paid_content: bool,
    pub is_featured: bool,
    pub reading_time: i32,
    pub word_count: i32,
    pub view_count: i64,
    pub clap_count: i64,
    pub comment_count: i64,
    pub bookmark_count: i64,
    pub share_count: i64,
    pub tags: Vec<TagInfo>,
    pub seo_title: Option<String>,
    pub seo_description: Option<String>,
    pub seo_keywords: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub is_bookmarked: Option<bool>, // 当前用户是否收藏
    pub is_clapped: Option<bool>,    // 当前用户是否点赞
    pub user_clap_count: Option<i32>, // 当前用户点赞次数
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArticleListItem {
    pub id: String,
    pub title: String,
    pub subtitle: Option<String>,
    pub slug: String,
    pub excerpt: Option<String>,
    pub cover_image_url: Option<String>,
    pub author: AuthorInfo,
    pub publication: Option<PublicationInfo>,
    pub status: ArticleStatus,
    pub is_paid_content: bool,
    pub is_featured: bool,
    pub reading_time: i32,
    pub view_count: i64,
    pub clap_count: i64,
    pub comment_count: i64,
    pub tags: Vec<TagInfo>,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorInfo {
    pub id: String,
    pub username: String,
    pub display_name: String,
    pub avatar_url: Option<String>,
    pub is_verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicationInfo {
    pub id: String,
    pub name: String,
    pub slug: String,
    pub logo_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SeriesInfo {
    pub id: String,
    pub title: String,
    pub slug: String,
    pub order: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagInfo {
    pub id: String,
    pub name: String,
    pub slug: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ArticleQuery {
    pub page: Option<usize>,
    pub limit: Option<usize>,
    pub status: Option<String>,
    pub author: Option<String>,
    pub publication: Option<String>,
    pub tag: Option<String>,
    pub featured: Option<bool>,
    pub search: Option<String>,
    pub sort: Option<String>, // "newest", "oldest", "popular", "trending"
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ArticleStats {
    pub total_articles: i64,
    pub published_articles: i64,
    pub draft_articles: i64,
    pub total_views: i64,
    pub total_claps: i64,
    pub total_comments: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TrendingArticle {
    pub article: ArticleListItem,
    pub trend_score: f64,
    pub growth_rate: f64,
}

impl Article {
    pub fn new(title: String, content: String, author_id: String) -> Self {
        let now = Utc::now();
        let slug = crate::utils::slug::generate_slug(&title);
        
        Self {
            id: Uuid::new_v4().to_string(),
            title,
            subtitle: None,
            slug,
            content: content.clone(),
            content_html: String::new(), // 将在服务层处理
            excerpt: None,
            cover_image_url: None,
            author_id,
            publication_id: None,
            series_id: None,
            series_order: None,
            status: ArticleStatus::Draft,
            is_paid_content: false,
            is_featured: false,
            reading_time: Self::calculate_reading_time(&content),
            word_count: Self::calculate_word_count(&content),
            view_count: 0,
            clap_count: 0,
            comment_count: 0,
            bookmark_count: 0,
            share_count: 0,
            seo_title: None,
            seo_description: None,
            seo_keywords: Vec::new(),
            metadata: serde_json::json!({}),
            created_at: now,
            updated_at: now,
            published_at: None,
            last_edited_at: None,
            is_deleted: false,
            deleted_at: None,
        }
    }

    fn calculate_word_count(content: &str) -> i32 {
        content.split_whitespace().count() as i32
    }

    fn calculate_reading_time(content: &str) -> i32 {
        let word_count = Self::calculate_word_count(content);
        // 假设每分钟阅读250个单词
        std::cmp::max(1, (word_count as f32 / 250.0).ceil() as i32)
    }

    pub fn update_content(&mut self, content: String) {
        self.content = content.clone();
        self.word_count = Self::calculate_word_count(&content);
        self.reading_time = Self::calculate_reading_time(&content);
        self.updated_at = Utc::now();
        self.last_edited_at = Some(Utc::now());
    }

    pub fn publish(&mut self) {
        if self.status == ArticleStatus::Draft {
            self.status = ArticleStatus::Published;
            self.published_at = Some(Utc::now());
            self.updated_at = Utc::now();
        }
    }

    pub fn unpublish(&mut self) {
        if self.status == ArticleStatus::Published {
            self.status = ArticleStatus::Draft;
            self.updated_at = Utc::now();
        }
    }

    pub fn archive(&mut self) {
        self.status = ArticleStatus::Archived;
        self.updated_at = Utc::now();
    }

    pub fn soft_delete(&mut self) {
        self.is_deleted = true;
        self.deleted_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    pub fn is_published(&self) -> bool {
        self.status == ArticleStatus::Published && !self.is_deleted
    }

    pub fn can_be_viewed_by_public(&self) -> bool {
        self.is_published() || self.status == ArticleStatus::Unlisted
    }
}

impl From<CreateArticleRequest> for Article {
    fn from(req: CreateArticleRequest) -> Self {
        let mut article = Article::new(req.title, req.content, String::new()); // author_id will be set in service

        article.subtitle = req.subtitle;
        article.excerpt = req.excerpt;
        article.cover_image_url = req.cover_image_url;
        article.publication_id = req.publication_id;
        article.series_id = req.series_id;
        article.series_order = req.series_order;
        article.is_paid_content = req.is_paid_content.unwrap_or(false);
        article.seo_title = req.seo_title;
        article.seo_description = req.seo_description;
        article.seo_keywords = req.seo_keywords.unwrap_or_default();

        // 创建接口总是创建草稿，通过单独的 publish 接口来发布
        // 忽略 save_as_draft 参数，保持向后兼容

        article
    }
}
