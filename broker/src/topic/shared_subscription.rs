use crate::topic::topic_storage::test_topic;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedSubscriptionFilter {
    pub share_name: String,
    pub topic_filter: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SharedSubscriptionFilterError {
    #[error("shared subscription share name is empty")]
    EmptyShareName,
    #[error("shared subscription topic filter is empty")]
    EmptyTopicFilter,
    #[error("shared subscription share name contains invalid wildcard or separator")]
    InvalidShareName,
    #[error("shared subscription topic filter is invalid: {0}")]
    InvalidTopicFilter(String),
}

fn is_valid_share_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    !name.contains('/') && !name.contains('+') && !name.contains('#')
}

pub fn parse_shared_subscription_filter(
    raw: &str,
) -> Result<Option<SharedSubscriptionFilter>, SharedSubscriptionFilterError> {
    let rest = match raw.strip_prefix("$share/") {
        Some(r) => r,
        None => return Ok(None),
    };

    let slash_pos = match rest.find('/') {
        Some(pos) => pos,
        None => return Err(SharedSubscriptionFilterError::EmptyTopicFilter),
    };

    let share_name = &rest[..slash_pos];
    let topic_filter = &rest[slash_pos + 1..];

    if !is_valid_share_name(share_name) {
        if share_name.is_empty() {
            return Err(SharedSubscriptionFilterError::EmptyShareName);
        }
        return Err(SharedSubscriptionFilterError::InvalidShareName);
    }

    if topic_filter.is_empty() {
        return Err(SharedSubscriptionFilterError::EmptyTopicFilter);
    }

    if !test_topic(topic_filter) {
        return Err(SharedSubscriptionFilterError::InvalidTopicFilter(
            topic_filter.to_string(),
        ));
    }

    Ok(Some(SharedSubscriptionFilter {
        share_name: share_name.to_string(),
        topic_filter: topic_filter.to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_filter_returns_none() {
        assert_eq!(parse_shared_subscription_filter("sensors/+"), Ok(None));
    }

    #[test]
    fn ordinary_filter_with_dollar_share_not_at_start_returns_none() {
        assert_eq!(parse_shared_subscription_filter("a/$share/b/c"), Ok(None));
    }

    #[test]
    fn valid_shared_filter() {
        let result = parse_shared_subscription_filter("$share/workers/sensors/+").unwrap();
        assert_eq!(
            result,
            Some(SharedSubscriptionFilter {
                share_name: "workers".to_string(),
                topic_filter: "sensors/+".to_string(),
            })
        );
    }

    #[test]
    fn valid_shared_filter_with_sys_topic() {
        let result = parse_shared_subscription_filter("$share/workers/$SYS/broker/#").unwrap();
        assert_eq!(
            result,
            Some(SharedSubscriptionFilter {
                share_name: "workers".to_string(),
                topic_filter: "$SYS/broker/#".to_string(),
            })
        );
    }

    #[test]
    fn empty_share_name() {
        assert_eq!(
            parse_shared_subscription_filter("$share//sensors/+"),
            Err(SharedSubscriptionFilterError::EmptyShareName)
        );
    }

    #[test]
    fn empty_topic_filter() {
        assert_eq!(
            parse_shared_subscription_filter("$share/workers/"),
            Err(SharedSubscriptionFilterError::EmptyTopicFilter)
        );
    }

    #[test]
    fn share_name_with_plus() {
        assert_eq!(
            parse_shared_subscription_filter("$share/worker+/sensors/+"),
            Err(SharedSubscriptionFilterError::InvalidShareName)
        );
    }

    #[test]
    fn share_name_with_hash() {
        assert_eq!(
            parse_shared_subscription_filter("$share/worker#/sensors/+"),
            Err(SharedSubscriptionFilterError::InvalidShareName)
        );
    }

    #[test]
    fn share_name_with_slash_is_valid() {
        // "work" is the share name, "ers/sensors/+" is the inner topic filter
        let result = parse_shared_subscription_filter("$share/work/ers/sensors/+").unwrap();
        assert_eq!(
            result,
            Some(SharedSubscriptionFilter {
                share_name: "work".to_string(),
                topic_filter: "ers/sensors/+".to_string(),
            })
        );
    }

    #[test]
    fn invalid_inner_topic_filter() {
        assert_eq!(
            parse_shared_subscription_filter("$share/workers/sport+"),
            Err(SharedSubscriptionFilterError::InvalidTopicFilter(
                "sport+".to_string()
            ))
        );
    }

    #[test]
    fn missing_topic_filter_after_share_name() {
        assert_eq!(
            parse_shared_subscription_filter("$share/workers"),
            Err(SharedSubscriptionFilterError::EmptyTopicFilter)
        );
    }

    #[test]
    fn valid_shared_filter_exact_topic() {
        let result = parse_shared_subscription_filter("$share/group/a/b/c").unwrap();
        assert_eq!(
            result,
            Some(SharedSubscriptionFilter {
                share_name: "group".to_string(),
                topic_filter: "a/b/c".to_string(),
            })
        );
    }
}
