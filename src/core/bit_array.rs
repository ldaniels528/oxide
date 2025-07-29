////////////////////////////////////////////////////////////////////
// BitArray module
////////////////////////////////////////////////////////////////////

use std::fmt;
use std::iter::FromIterator;

use serde::{Deserialize, Serialize};

const BITS_PER_ROW: usize = 64;

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct BitSet {
    array: Vec<u64>,
    floor: u64,
}

impl BitSet {
    pub fn from_vec(mut values: Vec<u64>) -> Self {
        values.sort();
        let mut bits = BitSet::new(values.len(), values[0]);
        bits.add(values.as_slice());
        bits
    }

    pub fn new(initial_size: usize, floor: u64) -> Self {
        Self {
            array: vec![0; initial_size],
            floor,
        }
    }

    pub fn decode(buf: &[u8]) -> Self {
        let floor = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let array_len = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
        let mut array = vec![0u64; array_len];
        for (i, chunk) in buf[12..].chunks(8).enumerate() {
            array[i] = u64::from_le_bytes(chunk.try_into().unwrap());
        }
        BitSet { array, floor }
    }

    pub fn is_on(bits: u64, col: usize) -> bool {
        (bits & BitSet::set_mask(col)) != 0
    }

    pub fn with_range(min_value: u64, max_value: u64) -> Self {
        let size = compute_array_size(min_value, max_value);
        Self::new(size, min_value)
    }

    pub fn add(&mut self, values: &[u64]) {
        let mut changed = false;
        for &value in values {
            let (row, col) = self.ensure_coordinates(value);
            let before = self.array[row];
            self.array[row] |= 1 << col;
            if before != self.array[row] {
                changed = true;
            }
        }
    }

    pub fn ascending(&self) -> Vec<u64> {
        let mut result = Vec::new();
        for (row, &bits) in self.array.iter().enumerate() {
            if bits != 0 {
                for col in 0..BITS_PER_ROW {
                    if BitSet::is_on(bits, col) {
                        result.push(self.to_value(row, col));
                    }
                }
            }
        }
        result
    }

    pub fn descending(&self) -> Vec<u64> {
        let mut result = Vec::new();
        for (row, &bits) in self.array.iter().enumerate().rev() {
            if bits != 0 {
                for col in (0..BITS_PER_ROW).rev() {
                    if BitSet::is_on(bits, col) {
                        result.push(self.to_value(row, col));
                    }
                }
            }
        }
        result
    }
    
    pub fn contains(&self, value: u64) -> bool {
        let (row, col) = self.to_coordinates(value);
        row < self.array.len() && (self.array[row] & (1 << col)) != 0
    }

    pub fn min_value(&self) -> Option<u64> {
        for (row, &bits) in self.array.iter().enumerate() {
            if bits != 0 {
                for col in 0..BITS_PER_ROW {
                    if BitSet::is_on(bits, col) {
                        return Some(self.to_value(row, col));
                    }
                }
            }
        }
        None
    }

    pub fn max_value(&self) -> Option<u64> {
        for (row, &bits) in self.array.iter().enumerate().rev() {
            if bits != 0 {
                for col in (0..BITS_PER_ROW).rev() {
                    if BitSet::is_on(bits, col) {
                        return Some(self.to_value(row, col));
                    }
                }
            }
        }
        None
    }

    pub fn remove(&mut self, value: u64) -> bool {
        let (row, col) = self.to_coordinates(value);
        if row >= self.array.len() {
            return false;
        }
        let before = self.array[row];
        self.array[row] &= !(1 << col);
        let changed = self.array[row] != before;
        changed
    }

    pub fn remove_all(&mut self) {
        self.array.fill(0);
    }

    pub fn replace_all(&mut self, values: &[u64]) {
        self.array.fill(0);
        if let Some(&min_value) = values.iter().min() {
            self.floor = min_value;
        }
        self.add(values);
    }

    pub fn size(&self) -> usize {
        self.array.iter().map(|bits| bits.count_ones() as usize).sum()
    }

    pub fn size_in_bytes(&self) -> usize {
        self.encode().len()
    }

    pub fn is_empty(&self) -> bool {
        self.array.iter().all(|&bits| bits == 0)
    }

    pub fn capacity(&self) -> usize {
        self.array.len()
    }

    pub fn foreach<F: FnMut(u64)>(&self, mut f: F) {
        for (row, &bits) in self.array.iter().enumerate() {
            if bits != 0 {
                for col in 0..BITS_PER_ROW {
                    if bits & (1 << col) != 0 {
                        f(self.to_value(row, col));
                    }
                }
            }
        }
    }

    pub fn set_mask(col: usize) -> u64 {
        1 << col
    }

    pub fn unset_mask(col: usize) -> u64 {
        !(1 << col)
    }

    pub fn to_coordinates(&self, value: u64) -> (usize, usize) {
        let v = (value - self.floor) as usize;
        (v / BITS_PER_ROW, v % BITS_PER_ROW)
    }

    pub fn to_vec(&self) -> Vec<u64> {
        let mut values = Vec::new();
        self.foreach(|value| values.push(value));
        values
    }


    pub fn to_string(&self) -> String {
        format!("BitSet({:?})", self.ascending())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        buf.extend_from_slice(&self.floor.to_le_bytes());
        buf.extend_from_slice(&(self.array.len() as i32).to_le_bytes());
        for &bits in &self.array {
            buf.extend_from_slice(&bits.to_le_bytes());
        }
        buf
    }

    fn encoded_size(&self) -> usize {
        std::mem::size_of::<u64>() + std::mem::size_of::<i32>() + self.array.len() * std::mem::size_of::<u64>()
    }

    fn ensure_coordinates(&mut self, value: u64) -> (usize, usize) {
        let (row, col) = self.to_coordinates(value);
        if row >= self.array.len() {
            self.grow_to_fit_upper_bound(value);
        } else if value < self.floor {
            self.grow_to_fit_lower_bound(value);
            return self.ensure_coordinates(value);
        }
        (row, col)
    }

    fn grow_to_fit_lower_bound(&mut self, value: u64) {
        let mut new_bit_array = BitSet::with_range(value, self.floor + self.array.len() as u64 * BITS_PER_ROW as u64);
        new_bit_array.add(&self.to_vec());
        *self = new_bit_array;
    }

    fn grow_to_fit_upper_bound(&mut self, value: u64) {
        let new_size = compute_array_size(self.floor, value + 1);
        if new_size > self.array.len() {
            self.array.resize(new_size, 0);
        }
    }

    fn to_value(&self, row: usize, col: usize) -> u64 {
        self.floor + row as u64 * BITS_PER_ROW as u64 + col as u64
    }
}

impl fmt::Debug for BitSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BitSet({:?})", self.ascending())
    }
}

fn compute_array_size(min_value: u64, max_value: u64) -> usize {
    let n = (max_value - min_value) as usize;
    n / BITS_PER_ROW + (if n % BITS_PER_ROW > 0 { 1 } else { 0 })
}

impl FromIterator<u64> for BitSet {
    fn from_iter<I: IntoIterator<Item=u64>>(iter: I) -> Self {
        let mut bit_array = BitSet::new(0, 0);
        for value in iter {
            bit_array.add(&[value]);
        }
        bit_array
    }
}

impl Extend<u64> for BitSet {
    fn extend<I: IntoIterator<Item=u64>>(&mut self, iter: I) {
        for value in iter {
            self.add(&[value]);
        }
    }
}

impl Ord for BitSet {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First compare floor values
        match self.floor.cmp(&other.floor) {
            std::cmp::Ordering::Equal => {
                // Then compare the bit arrays lexicographically
                self.array.cmp(&other.array)
            }
            ordering => ordering,
        }
    }
}

impl PartialOrd for BitSet {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_bitarray() {
        let bit_array = BitSet::new(10, 0);
        assert_eq!(bit_array.capacity(), 10);
        assert_eq!(bit_array.size(), 0);
        assert!(bit_array.is_empty());
    }

    #[test]
    fn test_add_and_contains() {
        let mut bit_array = BitSet::new(10, 0);
        bit_array.add(&[5, 10, 15]);

        assert!(bit_array.contains(5));
        assert!(bit_array.contains(10));
        assert!(bit_array.contains(15));
        assert!(!bit_array.contains(20));

        assert_eq!(bit_array.size(), 3);
        assert!(!bit_array.is_empty());
    }

    #[test]
    fn test_remove() {
        let mut bit_array = BitSet::new(10, 0);
        bit_array.add(&[5, 10, 15]);

        assert!(bit_array.remove(10));
        assert!(!bit_array.contains(10));
        assert!(bit_array.contains(5));
        assert!(bit_array.contains(15));

        assert_eq!(bit_array.size(), 2);
    }

    #[test]
    fn test_remove_nonexistent() {
        let mut bit_array = BitSet::new(10, 0);
        assert!(!bit_array.remove(42)); // Should return false since 42 isn't in the array.
    }

    #[test]
    fn test_remove_all() {
        let mut bit_array = BitSet::new(10, 0);
        bit_array.add(&[1, 2, 3, 4, 5]);
        bit_array.remove_all();

        assert_eq!(bit_array.size(), 0);
        assert!(bit_array.is_empty());
        assert!(!bit_array.contains(1));
    }

    #[test]
    fn test_foreach() {
        let mut bit_array = BitSet::new(10, 0);
        bit_array.add(&[1, 2, 3]);

        let mut sum = 0;
        bit_array.foreach(|value| sum += value);
        assert_eq!(sum, 6); // 1 + 2 + 3 = 6
    }

    #[test]
    fn test_encode_decode() {
        let mut bit_array = BitSet::new(10, 0);
        bit_array.add(&[5, 10, 15]);

        let encoded = bit_array.encode();
        let decoded = BitSet::decode(&encoded);

        assert_eq!(bit_array.to_vec(), decoded.to_vec());
    }

    #[test]
    fn test_extend() {
        let mut bit_array = BitSet::new(10, 0);
        bit_array.add(&[1, 2, 3]);

        bit_array.extend(vec![4, 5, 6]);
        for i in 1..=6 {
            assert!(bit_array.contains(i));
        }
    }

    #[test]
    fn test_iter_from_iter() {
        let values = vec![1, 2, 3, 4, 5];
        let bit_array: BitSet = values.iter().copied().collect();
        for value in values {
            assert!(bit_array.contains(value));
        }
    }

    #[test]
    fn test_debug_formatting() {
        let mut bit_array = BitSet::new(10, 0);
        bit_array.add(&[5, 10, 15]);

        let debug_str = format!("{:?}", bit_array);
        assert_eq!(debug_str, "BitSet([5, 10, 15])");
    }
}
