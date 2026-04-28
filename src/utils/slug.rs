use regex::Regex;
use once_cell::sync::Lazy;

static SLUG_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"[^a-zA-Z0-9\-_]").unwrap()
});

/// 从标题生成 URL 友好的 slug
pub fn generate_slug(title: &str) -> String {
    // 转换为小写
    let mut slug = title.to_lowercase();
    
    // 替换空格为连字符
    slug = slug.replace(' ', "-");
    
    // 移除所有非字母数字和连字符的字符
    slug = SLUG_REGEX.replace_all(&slug, "").to_string();
    
    // 移除连续的连字符
    let consecutive_hyphens = Regex::new(r"-+").unwrap();
    slug = consecutive_hyphens.replace_all(&slug, "-").to_string();
    
    // 移除开头和结尾的连字符
    slug = slug.trim_matches('-').to_string();
    
    // 限制长度
    if slug.len() > 100 {
        slug = slug.chars().take(100).collect();
        // 确保不会在单词中间截断
        if let Some(last_hyphen) = slug.rfind('-') {
            if last_hyphen > 50 { // 确保 slug 不会太短
                slug = slug[..last_hyphen].to_string();
            }
        }
    }
    
    // Use a short UUID suffix instead of "untitled" to avoid collisions
    if slug.is_empty() {
        let id = uuid::Uuid::new_v4().to_string();
        slug = format!("post-{}", &id[..8]);
    }

    slug
}

/// 为 slug 添加唯一后缀（如果需要的话）
pub fn make_slug_unique(base_slug: &str, existing_slugs: &[String]) -> String {
    let mut slug = base_slug.to_string();
    let mut counter = 1;
    
    while existing_slugs.contains(&slug) {
        slug = format!("{}-{}", base_slug, counter);
        counter += 1;
        
        // 防止无限循环
        if counter > 1000 {
            slug = format!("{}-{}", base_slug, uuid::Uuid::new_v4());
            break;
        }
    }
    
    slug
}

/// 验证 slug 格式是否正确
pub fn is_valid_slug(slug: &str) -> bool {
    if slug.is_empty() || slug.len() > 100 {
        return false;
    }
    
    // 检查是否只包含允许的字符
    let valid_chars = Regex::new(r"^[a-zA-Z0-9\-_]+$").unwrap();
    if !valid_chars.is_match(slug) {
        return false;
    }
    
    // 不能以连字符开头或结尾
    if slug.starts_with('-') || slug.ends_with('-') {
        return false;
    }
    
    // 不能包含连续的连字符
    if slug.contains("--") {
        return false;
    }
    
    true
}

/// 从现有 slug 提取基础名称（移除数字后缀）
pub fn extract_base_slug(slug: &str) -> String {
    let parts: Vec<&str> = slug.rsplitn(2, '-').collect();
    
    if parts.len() == 2 {
        // 检查最后一部分是否为数字
        if parts[0].parse::<u32>().is_ok() {
            return parts[1].to_string();
        }
    }
    
    slug.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_slug() {
        assert_eq!(generate_slug("Hello World"), "hello-world");
        assert_eq!(generate_slug("How to Build a Blog in Rust"), "how-to-build-a-blog-in-rust");
        assert_eq!(generate_slug("JavaScript: The Good Parts"), "javascript-the-good-parts");
        assert_eq!(generate_slug("Hello, World! How are you?"), "hello-world-how-are-you");
        assert_eq!(generate_slug(""), "untitled");
        assert_eq!(generate_slug("   "), "untitled");
        assert_eq!(generate_slug("---"), "untitled");
    }

    #[test]
    fn test_make_slug_unique() {
        let existing = vec![
            "hello-world".to_string(),
            "hello-world-1".to_string(),
            "hello-world-2".to_string(),
        ];
        
        assert_eq!(make_slug_unique("hello-world", &existing), "hello-world-3");
        assert_eq!(make_slug_unique("new-post", &existing), "new-post");
    }

    #[test]
    fn test_is_valid_slug() {
        assert!(is_valid_slug("hello-world"));
        assert!(is_valid_slug("hello_world"));
        assert!(is_valid_slug("hello123"));
        
        assert!(!is_valid_slug(""));
        assert!(!is_valid_slug("-hello"));
        assert!(!is_valid_slug("hello-"));
        assert!(!is_valid_slug("hello--world"));
        assert!(!is_valid_slug("hello world"));
        assert!(!is_valid_slug("hello@world"));
    }

    #[test]
    fn test_extract_base_slug() {
        assert_eq!(extract_base_slug("hello-world"), "hello-world");
        assert_eq!(extract_base_slug("hello-world-1"), "hello-world");
        assert_eq!(extract_base_slug("hello-world-123"), "hello-world");
        assert_eq!(extract_base_slug("hello-world-abc"), "hello-world-abc");
    }
}