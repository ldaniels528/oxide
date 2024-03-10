////////////////////////////////////////////////////////////////////
// codec module - responsible for encoding/decoding bytes
////////////////////////////////////////////////////////////////////

use std::io;
use std::mem::size_of;

use uuid::Uuid;

pub fn decode_row_id(buffer: &Vec<u8>, offset: usize) -> usize {
    let mut id_array = [0u8; 8];
    id_array.copy_from_slice(&buffer[offset..(offset + 8)]);
    usize::from_be_bytes(id_array)
}

pub fn decode_string(buffer: &Vec<u8>, offset: usize, max_size: usize) -> String {
    let a: usize = offset + size_of::<usize>();
    let b: usize = a + max_size;
    let data: &[u8] = &buffer[a..b];
    let value = std::str::from_utf8(&*data).unwrap().chars()
        .filter(|&c| c != '\0')
        .collect();
    value
}

pub(crate) fn decode_u8<A>(buffer: &Vec<u8>, offset: usize, f: fn(u8) -> A) -> A {
    f(buffer[offset])
}

pub(crate) fn decode_u8x2<A>(buffer: &Vec<u8>, offset: usize, f: fn([u8; 2]) -> A) -> A {
    let mut scratch = [0; 2];
    let limit = offset + scratch.len();
    scratch.copy_from_slice(&buffer[offset..limit]);
    f(scratch)
}

pub(crate) fn decode_u8x4<A>(buffer: &Vec<u8>, offset: usize, f: fn([u8; 4]) -> A) -> A {
    let mut scratch = [0; 4];
    let limit = offset + scratch.len();
    scratch.copy_from_slice(&buffer[offset..limit]);
    f(scratch)
}

pub(crate) fn decode_u8x8<A>(buffer: &Vec<u8>, offset: usize, f: fn([u8; 8]) -> A) -> A {
    let mut scratch = [0; 8];
    let limit = offset + scratch.len();
    scratch.copy_from_slice(&buffer[offset..limit]);
    f(scratch)
}

pub(crate) fn decode_u8x16<A>(buffer: &Vec<u8>, offset: usize, f: fn([u8; 16]) -> A) -> A {
    let mut scratch = [0; 16];
    let limit = offset + scratch.len();
    scratch.copy_from_slice(&buffer[offset..limit]);
    f(scratch)
}

pub(crate) fn decode_uuid(uuid_str: &str) -> io::Result<[u8; 16]> {
    Ok(*Uuid::parse_str(uuid_str)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?.as_bytes())
}

pub fn encode_chars(chars: Vec<char>) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(chars.len());
    for ch in chars {
        buf.extend(ch.encode_utf8(&mut [0; 4]).bytes());
    }
    encode_u8x_n(buf)
}

pub fn encode_row_id(id: usize) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

pub fn encode_string(string: &str) -> Vec<u8> {
    encode_u8x_n(string.bytes().collect())
}

pub fn encode_u8x_n(bytes: Vec<u8>) -> Vec<u8> {
    let width = bytes.len();
    let overhead = width.to_be_bytes();
    let mut buf: Vec<u8> = Vec::with_capacity(width + overhead.len());
    buf.extend(width.to_be_bytes());
    buf.extend(bytes);
    buf
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_row_id() {
        let buf: Vec<u8> = vec![0xDE, 0xAD, 0xCA, 0xFE, 0xBE, 0xEF, 0xBA, 0xBE];
        let id = decode_row_id(&buf, 0);
        assert_eq!(id, 0xDEAD_CAFE_BEEF_BABE)
    }

    #[test]
    fn test_decode_u8() {
        let buf: Vec<u8> = vec![64];
        let value = decode_u8(&buf, 0, |b| b);
        assert_eq!(value, 64)
    }

    #[test]
    fn test_decode_u8x2() {
        let buf: Vec<u8> = vec![255, 255];
        let value: u16 = decode_u8x2(&buf, 0, |b| u16::from_be_bytes(b));
        assert_eq!(value, 65535)
    }

    #[test]
    fn test_decode_u8x4() {
        let buf: Vec<u8> = vec![64, 64, 112, 163];
        let value: i32 = decode_u8x4(&buf, 0, |b| i32::from_be_bytes(b));
        assert_eq!(value, 1077964963)
    }

    #[test]
    fn test_decode_u8x8() {
        let buf: Vec<u8> = vec![64, 64, 112, 163, 215, 10, 61, 113];
        let value: f64 = decode_u8x8(&buf, 0, |b| f64::from_be_bytes(b));
        assert_eq!(value, 32.88)
    }

    #[test]
    fn test_decode_u8x16() {
        let buf: Vec<u8> = vec![0x29, 0x92, 0xbb, 0x53, 0xcc, 0x3c, 0x4f, 0x30, 0x8a, 0x4c, 0xc1, 0xa6, 0x66, 0xaf, 0xcc, 0x46];
        let value: Uuid = decode_u8x16(&buf, 0, |b| Uuid::from_bytes(b));
        assert_eq!(value, Uuid::parse_str("2992bb53-cc3c-4f30-8a4c-c1a666afcc46").unwrap())
    }

    #[test]
    fn test_decode_string_max_length() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 4, b'Y', b'H', b'W', b'H'];
        assert_eq!(decode_string(&buf, 0, 4), "YHWH")
    }

    #[test]
    fn test_decode_string_less_than_max_length() {
        let buf: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 4, b'Y', b'A', b'H', 0];
        assert_eq!(decode_string(&buf, 0, 4), "YAH")
    }

    #[test]
    fn test_decode_uuid() {
        let bytes: [u8; 16] = decode_uuid("2992bb53-cc3c-4f30-8a4c-c1a666afcc46").unwrap();
        assert_eq!(bytes, [0x29, 0x92, 0xbb, 0x53, 0xcc, 0x3c, 0x4f, 0x30, 0x8a, 0x4c, 0xc1, 0xa6, 0x66, 0xaf, 0xcc, 0x46])
    }

    #[test]
    fn test_encode_chars() {
        let expected: Vec<u8> = vec![0, 0, 0, 0, 0, 0, 0, 30, 84, 104, 101, 32, 108, 105, 116, 116, 108, 101, 32, 121, 111, 114, 107, 105, 101, 32, 98, 97, 114, 107, 101, 100, 32, 97, 116, 32, 109, 101];
        let actual: Vec<u8> = encode_chars("The little yorkie barked at me".chars().collect());
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_encode_row_id() {
        let expected: Vec<u8> = vec![0xDE, 0xAD, 0xCA, 0xFE, 0xBE, 0xEF, 0xBA, 0xBE];
        let id = 0xDEAD_CAFE_BEEF_BABE;
        assert_eq!(encode_row_id(id), expected)
    }
}