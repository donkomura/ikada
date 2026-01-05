use crate::memcache::error::Result;

#[derive(Debug, PartialEq, Clone)]
pub enum MemcacheCommand {
    Set {
        key: String,
        flags: u32,
        exptime: u32,
        bytes: usize,
        data: Vec<u8>,
    },
    Get {
        keys: Vec<String>,
    },
    Delete {
        key: String,
    },
}

#[derive(Debug, PartialEq)]
pub enum MemcacheResponse {
    Stored,
    NotStored,
    Value {
        key: String,
        flags: u32,
        data: Vec<u8>,
    },
    End,
    Deleted,
    NotFound,
    Error(String),
    ClientError(String),
}

impl MemcacheCommand {
    pub fn parse(_input: &str) -> Result<Self> {
        todo!()
    }
}

impl MemcacheResponse {
    pub fn serialize(&self) -> String {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_set_command() {
        let input = "set mykey 0 0 5\r\nhello\r\n";
        let result = MemcacheCommand::parse(input).unwrap();

        assert_eq!(
            result,
            MemcacheCommand::Set {
                key: "mykey".to_string(),
                flags: 0,
                exptime: 0,
                bytes: 5,
                data: b"hello".to_vec(),
            }
        );
    }

    #[test]
    fn test_parse_set_command_with_flags() {
        let input = "set testkey 42 3600 4\r\ndata\r\n";
        let result = MemcacheCommand::parse(input).unwrap();

        assert_eq!(
            result,
            MemcacheCommand::Set {
                key: "testkey".to_string(),
                flags: 42,
                exptime: 3600,
                bytes: 4,
                data: b"data".to_vec(),
            }
        );
    }

    #[test]
    fn test_parse_get_command_single_key() {
        let input = "get mykey\r\n";
        let result = MemcacheCommand::parse(input).unwrap();

        assert_eq!(
            result,
            MemcacheCommand::Get {
                keys: vec!["mykey".to_string()],
            }
        );
    }

    #[test]
    fn test_parse_get_command_multiple_keys() {
        let input = "get key1 key2 key3\r\n";
        let result = MemcacheCommand::parse(input).unwrap();

        assert_eq!(
            result,
            MemcacheCommand::Get {
                keys: vec![
                    "key1".to_string(),
                    "key2".to_string(),
                    "key3".to_string()
                ],
            }
        );
    }

    #[test]
    fn test_parse_delete_command() {
        let input = "delete mykey\r\n";
        let result = MemcacheCommand::parse(input).unwrap();

        assert_eq!(
            result,
            MemcacheCommand::Delete {
                key: "mykey".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_invalid_command() {
        let input = "invalid\r\n";
        let result = MemcacheCommand::parse(input);

        assert!(result.is_err());
    }

    #[test]
    fn test_serialize_stored_response() {
        let response = MemcacheResponse::Stored;
        assert_eq!(response.serialize(), "STORED\r\n");
    }

    #[test]
    fn test_serialize_not_stored_response() {
        let response = MemcacheResponse::NotStored;
        assert_eq!(response.serialize(), "NOT_STORED\r\n");
    }

    #[test]
    fn test_serialize_value_response() {
        let response = MemcacheResponse::Value {
            key: "mykey".to_string(),
            flags: 0,
            data: b"hello".to_vec(),
        };
        assert_eq!(response.serialize(), "VALUE mykey 0 5\r\nhello\r\n");
    }

    #[test]
    fn test_serialize_end_response() {
        let response = MemcacheResponse::End;
        assert_eq!(response.serialize(), "END\r\n");
    }

    #[test]
    fn test_serialize_deleted_response() {
        let response = MemcacheResponse::Deleted;
        assert_eq!(response.serialize(), "DELETED\r\n");
    }

    #[test]
    fn test_serialize_not_found_response() {
        let response = MemcacheResponse::NotFound;
        assert_eq!(response.serialize(), "NOT_FOUND\r\n");
    }

    #[test]
    fn test_serialize_error_response() {
        let response = MemcacheResponse::Error("command not found".to_string());
        assert_eq!(response.serialize(), "ERROR\r\n");
    }

    #[test]
    fn test_serialize_client_error_response() {
        let response =
            MemcacheResponse::ClientError("bad data chunk".to_string());
        assert_eq!(response.serialize(), "CLIENT_ERROR bad data chunk\r\n");
    }
}
