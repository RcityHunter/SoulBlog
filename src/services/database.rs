use crate::config::Config;
use crate::error::{AppError, Result};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::any::TypeId;
use std::sync::Arc;
use std::fmt::Debug;
use soulcore::prelude::*;
use soulcore::engines::storage::StorageEngine;
use surrealdb::types::ToSql;
use surrealdb::types::Value as SurrealValue;
use tracing::{info, error, debug};

/// 兼容 SurrealDB 3 的查询响应包装器
pub struct QueryResponse {
    inner: surrealdb::IndexedResults,
}

impl QueryResponse {
    pub fn new(inner: surrealdb::IndexedResults) -> Self {
        Self { inner }
    }

    /// 按索引提取结果并反序列化为目标类型
    pub fn take<T: DeserializeOwned + 'static>(&mut self, index: usize) -> Result<T> {
        let raw: SurrealValue = self
            .inner
            .take(index)
            .map_err(|e| AppError::Database(surrealdb::Error::thrown(e.to_string())))?;
        let json_raw = serde_json::to_value(raw).map_err(|e| AppError::Internal(e.to_string()))?;

        // Keep original payload shape for callers that explicitly ask for SurrealValue.
        if TypeId::of::<T>() == TypeId::of::<SurrealValue>() {
            return serde_json::from_value(json_raw).map_err(|e| AppError::Internal(e.to_string()));
        }

        let json = normalize_query_json(json_raw);

        // SurrealDB v3 can return either a single object or an array depending on statement shape.
        // Many legacy callsites still deserialize into Vec<T>; transparently handle object -> [object].
        match serde_json::from_value(json.clone()) {
            Ok(v) => Ok(v),
            Err(first_err) => {
                // object -> [object] for Vec<T>
                if json.is_object() {
                    let wrapped = serde_json::Value::Array(vec![json.clone()]);
                    match serde_json::from_value(wrapped) {
                        Ok(v) => return Ok(v),
                        Err(second_err) => {
                            return Err(AppError::Internal(format!(
                                "query decode failed: first={}, object_to_array={}",
                                first_err, second_err
                            )));
                        }
                    }
                }

                // [object] -> object for T
                if let Some(arr) = json.as_array() {
                    if arr.len() == 1 {
                        match serde_json::from_value(arr[0].clone()) {
                            Ok(v) => return Ok(v),
                            Err(second_err) => {
                                return Err(AppError::Internal(format!(
                                    "query decode failed: first={}, array_to_object={}",
                                    first_err, second_err
                                )));
                            }
                        }
                    }
                }

                Err(AppError::Internal(first_err.to_string()))
            }
        }
    }
}

fn normalize_query_json(mut v: serde_json::Value) -> serde_json::Value {
    v = detag_surreal_json(v);

    // Unwrap common envelope shapes from SurrealDB client/result adapters.
    // Prefer wrapper keys even if extra metadata keys exist.
    loop {
        let Some(obj) = v.as_object() else {
            break;
        };
        if let Some(next) = obj.get("result").cloned() {
            v = next;
            continue;
        }
        if let Some(next) = obj.get("data").cloned() {
            v = next;
            continue;
        }
        if let Some(next) = obj.get("value").cloned() {
            v = next;
            continue;
        }
        if obj.len() == 1 {
            if let Some(next) = obj.get("0").cloned() {
                v = next;
                continue;
            }
        }
        break;
    }
    v
}

fn detag_surreal_json(v: serde_json::Value) -> serde_json::Value {
    use serde_json::{Map, Number, Value};

    match v {
        Value::Array(arr) => Value::Array(arr.into_iter().map(detag_surreal_json).collect()),
        Value::Object(mut obj) => {
            if obj.len() == 1 {
                if let Some(inner) = obj.remove("Array") {
                    if let Value::Array(arr) = inner {
                        return Value::Array(arr.into_iter().map(detag_surreal_json).collect());
                    }
                    return detag_surreal_json(inner);
                }
                if let Some(inner) = obj.remove("Object") {
                    if let Value::Object(map) = inner {
                        let mapped: Map<String, Value> = map
                            .into_iter()
                            .map(|(k, v)| (k, detag_surreal_json(v)))
                            .collect();
                        return Value::Object(mapped);
                    }
                    return detag_surreal_json(inner);
                }
                if let Some(inner) = obj.remove("String") {
                    return Value::String(inner.as_str().unwrap_or_default().to_string());
                }
                if let Some(inner) = obj.remove("Bool") {
                    return Value::Bool(inner.as_bool().unwrap_or(false));
                }
                if obj.contains_key("Null") {
                    return Value::Null;
                }
                if let Some(inner) = obj.remove("Datetime") {
                    // Keep datetime as RFC3339 string for model deserialization.
                    return Value::String(inner.as_str().unwrap_or_default().to_string());
                }
                if let Some(inner) = obj.remove("Uuid") {
                    return Value::String(inner.as_str().unwrap_or_default().to_string());
                }
                if let Some(inner) = obj.remove("Number") {
                    if let Value::Object(mut n) = inner {
                        if let Some(i) = n.remove("Int").and_then(|x| x.as_i64()) {
                            return Value::Number(Number::from(i));
                        }
                        if let Some(u) = n.remove("Uint").and_then(|x| x.as_u64()) {
                            return Value::Number(Number::from(u));
                        }
                        if let Some(f) = n.remove("Float").and_then(|x| x.as_f64()) {
                            if let Some(num) = Number::from_f64(f) {
                                return Value::Number(num);
                            }
                        }
                        if let Some(dec) = n.remove("Decimal") {
                            if let Some(s) = dec.as_str() {
                                if let Ok(f) = s.parse::<f64>() {
                                    if let Some(num) = Number::from_f64(f) {
                                        return Value::Number(num);
                                    }
                                }
                                return Value::String(s.to_string());
                            }
                        }
                        return Value::Object(
                            n.into_iter()
                                .map(|(k, v)| (k, detag_surreal_json(v)))
                                .collect(),
                        );
                    }
                    return detag_surreal_json(inner);
                }
            }

            Value::Object(
                obj.into_iter()
                    .map(|(k, v)| (k, detag_surreal_json(v)))
                    .collect(),
            )
        }
        other => other,
    }
}

/// 数据库服务
#[derive(Clone)]
pub struct Database {
    pub storage: Arc<StorageEngine>,
    pub config: Config,
}

impl Database {
    /// 创建新的数据库实例
    pub async fn new(config: &Config) -> Result<Self> {
        info!("Initializing database connection to {}", config.database_url);
        
        // 创建存储配置
        let storage_config = StorageConfig {
            connection_mode: ConnectionMode::Http,
            url: config.database_url.clone(),
            username: config.database_username.clone(),
            password: config.database_password.clone(),
            namespace: config.database_namespace.clone(),
            database: config.database_name.clone(),
            pool_size: 10,
            ..Default::default()
        };

        // 使用SoulCoreBuilder创建storage engine
        let soulcore = SoulCoreBuilder::new()
            .with_storage_config(storage_config)
            .build()
            .await
            .map_err(|e| AppError::from(e))?;

        let storage = soulcore.storage().clone();

        Ok(Self {
            storage,
            config: config.clone(),
        })
    }

    /// 验证数据库连接
    pub async fn verify_connection(&self) -> Result<()> {
        match self.storage.query("INFO FOR DB").await {
            Ok(_) => {
                info!("Database connection verified successfully");
                Ok(())
            }
            Err(e) => {
                error!("Failed to verify database connection: {}", e);
                Err(AppError::from(e))
            }
        }
    }

    /// 初始化所有表 schema（SurrealDB 3.0 要求表必须先定义才能 SELECT）
    pub async fn initialize_schema(&self) -> Result<()> {
        let tables = [
            "user_auth", "user_profile", "article", "tag", "article_tag",
            "comment", "clap", "follow", "bookmark", "notification",
            "publication", "publication_member", "series", "series_article",
            "subscription", "payment", "revenue", "media", "domain",
            "recommendation", "analytics_event", "search_index",
            "user_tag_follow", "article_view", "article_share",
            "publication_revenue", "stripe_customer", "stripe_account",
            "ai_config",
        ];
        let mut sql = String::new();
        for table in &tables {
            sql.push_str(&format!("DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;\n", table));
        }
        // Ensure datetime defaults for key tables (OVERWRITE to update if already defined)
        for table in &["article", "user_profile", "comment", "tag"] {
            sql.push_str(&format!(
                "DEFINE FIELD OVERWRITE created_at ON TABLE {} DEFAULT time::now();\n",
                table
            ));
            sql.push_str(&format!(
                "DEFINE FIELD OVERWRITE updated_at ON TABLE {} DEFAULT time::now();\n",
                table
            ));
        }
        self.storage.query(&sql).await.map_err(AppError::from)?;
        info!("Schema initialized: {} tables + datetime defaults defined", tables.len());
        Ok(())
    }

    /// 使用查询构建器创建查询
    pub fn query_builder(&self) -> QueryBuilder {
        self.storage.query_builder()
    }
    
    /// 执行原始SQL查询
    pub async fn query(&self, sql: &str) -> Result<QueryResponse> {
        let inner = self.storage.query(sql)
            .await
            .map_err(AppError::from)?;
        Ok(QueryResponse::new(inner))
    }

    /// 执行带参数的查询
    pub async fn query_with_params<P>(&self, sql: &str, params: P) -> Result<QueryResponse>
    where
        P: Serialize,
    {
        let params = serde_json::to_value(params)
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let inner = self.storage.query_with_params(sql, params)
            .await
            .map_err(AppError::from)?;
        Ok(QueryResponse::new(inner))
    }

    /// 创建记录
    pub async fn create<T>(&self, table: &str, data: T) -> Result<T>
    where
        T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + Debug + 'static,
    {
        let query = format!("CREATE {} CONTENT $data RETURN *", table);
        let mut response = self.query_with_params(&query, json!({ "data": data })).await?;
        let results: Vec<T> = response.take(0)?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| AppError::Internal("Failed to create record".to_string()))
    }

    /// 选择记录
    pub async fn select<T>(&self, resource: &str) -> Result<Vec<T>>
    where
        T: for<'de> Deserialize<'de> + Send + Sync + Debug + 'static,
    {
        let query = format!("SELECT * FROM {}", resource);
        let mut response = self.query(&query).await?;
        let results: Vec<T> = response.take(0)?;
        Ok(results)
    }

    /// 更新记录
    pub async fn update<T>(&self, thing: Thing, data: T) -> Result<Option<T>>
    where
        T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Debug + 'static,
    {
        let query = format!("UPDATE {} CONTENT $data RETURN *", thing.to_sql());
        let mut response = self.query_with_params(&query, json!({ "data": data })).await?;
        let results: Vec<T> = response.take(0)?;
        Ok(results.into_iter().next())
    }

    /// 删除记录
    pub async fn delete(&self, thing: Thing) -> Result<()> {
        let query = format!("DELETE {}", thing.to_sql());
        self.query(&query).await?;
        Ok(())
    }

    /// 通过ID删除记录
    pub async fn delete_by_id(&self, table: &str, id: &str) -> Result<()> {
        let prefix = format!("{}:", table);
        let pure_id = if id.starts_with(&prefix) { &id[prefix.len()..] } else { id };
        let thing = Thing::new(table, pure_id);
        self.delete(thing).await
    }

    /// 通过ID获取单个记录
    pub async fn get_by_id<T>(&self, table: &str, id: &str) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de> + Send + Sync + Debug + 'static,
    {
        // 获取纯 ID（不带 table 前缀）
        let prefix = format!("{}:", table);
        let pure_id = if id.starts_with(&prefix) {
            &id[prefix.len()..]
        } else {
            id
        };
        
        // 使用反引号包裹 ID 以避免解析问题（与 article.rs 保持一致）
        let query = format!("SELECT * FROM {}:`{}`", table, pure_id);
        debug!("Executing query: {}", query);
        
        let mut response = self.query(&query).await?;
        let results: Vec<T> = response.take(0)?;
        Ok(results.into_iter().next())
    }

    /// 通过ID更新记录
    pub async fn update_by_id<T>(&self, table: &str, id: &str, data: T) -> Result<Option<T>>
    where
        T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Debug + 'static,
    {
        let prefix = format!("{}:", table);
        let pure_id = if id.starts_with(&prefix) { &id[prefix.len()..] } else { id };
        let thing = Thing::new(table, pure_id);
        self.update(thing, data).await
    }

    /// 通过ID使用JSON数据更新记录并返回指定类型
    pub async fn update_by_id_with_json<T>(&self, table: &str, id: &str, updates: serde_json::Value) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de> + Send + Sync + Debug + 'static,
    {
        // 获取纯 ID（不带 table 前缀），并用反引号包裹，兼容包含连字符的 ID
        let prefix = format!("{}:", table);
        let pure_id = if id.starts_with(&prefix) { &id[prefix.len()..] } else { id };
        let query = format!("UPDATE {}:`{}` MERGE $updates RETURN *", table, pure_id);
        let mut response = self.query_with_params(&query, json!({"updates": updates})).await?;
        let results: Vec<T> = response.take(0)?;
        Ok(results.into_iter().next())
    }

    /// 查找单个记录
    pub async fn find_one<T>(&self, table: &str, field: &str, value: &str) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de> + Send + Sync + Clone + Debug + 'static,
    {
        let (query, params) = if field == "id" {
            let full_id = if value.starts_with(&format!("{}:", table)) {
                value.to_string()
            } else {
                format!("{}:{}", table, value)
            };
            (
                format!("SELECT * FROM {} WHERE id = type::thing($value)", table),
                json!({ "value": full_id }),
            )
        } else {
            (
                format!("SELECT * FROM {} WHERE {} = $value", table, field),
                json!({ "value": value }),
            )
        };

        let mut response = self.query_with_params(&query, params).await?;
        let records: Vec<T> = response.take(0)?;
        Ok(records.into_iter().next())
    }

}

/// 分页结果结构
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PaginatedResult<T> {
    pub data: Vec<T>,
    pub total: usize,
    pub page: usize,
    pub per_page: usize,
    pub total_pages: usize,
}

// 为了向后兼容，提供ClientWrapper别名
pub type ClientWrapper = Database;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_database_connection() {
        let config = Config::default();
        let db = Database::new(&config).await;
        assert!(db.is_ok());
    }
}
