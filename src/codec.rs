////////////////////////////////////////////////////////////////////
// codec - responsible for encoding/decoding bytes
////////////////////////////////////////////////////////////////////

use std::mem::size_of;

pub fn decode_row_id(buffer: &Vec<u8>, offset: usize) -> u64 {
    let mut id_chk_array: [u8; 8] = [0; 8];
    id_chk_array.copy_from_slice(&buffer[offset..(offset + 8)]);
    u64::from_be_bytes(id_chk_array)
}

pub fn decode_string(buffer: &Vec<u8>, offset: usize, max_size: usize) -> &str {
    let a: usize = offset + size_of::<usize>();
    let b: usize = a + max_size;
    let data: &[u8] = &buffer[a..b];
    let value = std::str::from_utf8(&*data).unwrap();
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

pub fn encode_chars(chars: Vec<char>) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(chars.len());
    for ch in chars {
        buf.extend(ch.encode_utf8(&mut [0; 4]).bytes());
    }
    encode_u8x_n(buf)
}

pub fn encode_row_id(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
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
        let id: u64 = decode_row_id(&buf, 0);
        assert_eq!(id, 0xDEAD_CAFE_BEEF_BABE)
    }

    #[test]
    fn test_decode_u8x8() {
        let buf: Vec<u8> = vec![64, 64, 112, 163, 215, 10, 61, 113];
        let value: f64 = decode_u8x8(&buf, 0, |b| f64::from_be_bytes(b));
        assert_eq!(value, 32.88)
    }

    #[test]
    fn test_encode_row_id() {
        let expected: Vec<u8> = vec![0xDE, 0xAD, 0xCA, 0xFE, 0xBE, 0xEF, 0xBA, 0xBE];
        let id: u64 = 0xDEAD_CAFE_BEEF_BABE;
        let actual: Vec<u8> = encode_row_id(id);
        assert_eq!(actual, expected)
    }
    
}