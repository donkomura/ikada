use crate::memcache::error::{MemcacheError, Result};
use nom::{
    IResult,
    branch::alt,
    bytes::complete::{tag, take, take_till, take_while1},
    character::complete::{digit1, line_ending, space1},
    combinator::{map, map_res},
    sequence::terminated,
};

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

fn parse_u32(input: &[u8]) -> IResult<&[u8], u32> {
    map_res(map_res(digit1, std::str::from_utf8), |s: &str| {
        s.parse::<u32>()
    })(input)
}

fn parse_usize(input: &[u8]) -> IResult<&[u8], usize> {
    map_res(map_res(digit1, std::str::from_utf8), |s: &str| {
        s.parse::<usize>()
    })(input)
}

fn is_not_space_or_crlf(c: u8) -> bool {
    c != b' ' && c != b'\r' && c != b'\n'
}

fn parse_key(input: &[u8]) -> IResult<&[u8], String> {
    map(
        map_res(take_while1(is_not_space_or_crlf), std::str::from_utf8),
        |s: &str| s.to_string(),
    )(input)
}

fn parse_set_command(input: &[u8]) -> IResult<&[u8], MemcacheCommand> {
    let (input, _) = tag(b"set ")(input)?;
    let (input, key) = parse_key(input)?;
    let (input, _) = space1(input)?;
    let (input, flags) = parse_u32(input)?;
    let (input, _) = space1(input)?;
    let (input, exptime) = parse_u32(input)?;
    let (input, _) = space1(input)?;
    let (input, bytes) = parse_usize(input)?;
    let (input, _) = line_ending(input)?;
    let (input, data) = take(bytes)(input)?;
    let (input, _) = line_ending(input)?;

    Ok((
        input,
        MemcacheCommand::Set {
            key,
            flags,
            exptime,
            bytes,
            data: data.to_vec(),
        },
    ))
}

fn parse_get_command(input: &[u8]) -> IResult<&[u8], MemcacheCommand> {
    let (input, _) = tag(b"get ")(input)?;
    let (input, keys_bytes) = take_till(|c| c == b'\r')(input)?;
    let (input, _) = line_ending(input)?;

    let keys_str = std::str::from_utf8(keys_bytes).map_err(|_| {
        nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Char,
        ))
    })?;
    let keys: Vec<String> =
        keys_str.split(' ').map(|s| s.to_string()).collect();

    Ok((input, MemcacheCommand::Get { keys }))
}

fn parse_delete_command(input: &[u8]) -> IResult<&[u8], MemcacheCommand> {
    let (input, _) = tag(b"delete ")(input)?;
    let (input, key) = terminated(parse_key, line_ending)(input)?;

    Ok((input, MemcacheCommand::Delete { key }))
}

fn parse_command(input: &[u8]) -> IResult<&[u8], MemcacheCommand> {
    alt((parse_set_command, parse_get_command, parse_delete_command))(input)
}

impl MemcacheCommand {
    pub fn parse(input: &str) -> Result<Self> {
        let bytes = input.as_bytes();
        match parse_command(bytes) {
            Ok((_, cmd)) => Ok(cmd),
            Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => Err(
                MemcacheError::Protocol(format!("Parse error: {:?}", e.code)),
            ),
            Err(nom::Err::Incomplete(_)) => {
                Err(MemcacheError::Protocol("Incomplete input".to_string()))
            }
        }
    }
}

impl MemcacheResponse {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            MemcacheResponse::Stored => b"STORED\r\n".to_vec(),
            MemcacheResponse::NotStored => b"NOT_STORED\r\n".to_vec(),
            MemcacheResponse::Value { key, flags, data } => {
                let header =
                    format!("VALUE {} {} {}\r\n", key, flags, data.len());
                let mut result = header.into_bytes();
                result.extend_from_slice(data);
                result.extend_from_slice(b"\r\n");
                result
            }
            MemcacheResponse::End => b"END\r\n".to_vec(),
            MemcacheResponse::Deleted => b"DELETED\r\n".to_vec(),
            MemcacheResponse::NotFound => b"NOT_FOUND\r\n".to_vec(),
            MemcacheResponse::Error(_) => b"ERROR\r\n".to_vec(),
            MemcacheResponse::ClientError(msg) => {
                format!("CLIENT_ERROR {}\r\n", msg).into_bytes()
            }
        }
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
    fn test_parse_set_command_with_binary_data() {
        let input = b"set binkey 0 0 5\r\n\x00\x01\x02\x03\x04\r\n";
        let input_str = std::str::from_utf8(input).unwrap();
        let result = MemcacheCommand::parse(input_str).unwrap();

        assert_eq!(
            result,
            MemcacheCommand::Set {
                key: "binkey".to_string(),
                flags: 0,
                exptime: 0,
                bytes: 5,
                data: vec![0x00, 0x01, 0x02, 0x03, 0x04],
            }
        );
    }
}
