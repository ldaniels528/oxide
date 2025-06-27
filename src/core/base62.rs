////////////////////////////////////////////////////////////////////
// Base62 module
////////////////////////////////////////////////////////////////////

use std::collections::HashMap;

struct Base62Converter {
    char_map: HashMap<char, u32>,
    reverse_char_map: Vec<char>, // Reverse map for decimal to base-62 conversion
}

impl Base62Converter {
    // Constructor to initialize the character maps
    fn new() -> Self {
        let chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        let mut char_map = HashMap::new();
        let mut reverse_char_map = Vec::new();

        // Create the forward map (char -> value) and reverse map (value -> char)
        for (i, ch) in chars.chars().enumerate() {
            char_map.insert(ch, i as u32);
            reverse_char_map.push(ch);
        }

        Base62Converter {
            char_map,
            reverse_char_map,
        }
    }

    // Convert a base-62 string to a decimal number
    fn base_n_to_decimal(&self, alphanumeric_str: &str) -> u128 {
        alphanumeric_str.chars().fold(0, |acc, ch| {
            acc * 62 + *self.char_map.get(&ch).unwrap_or(&0) as u128
        })
    }

    // Convert a decimal number to a base-62 string, padded to 'length' characters
    fn decimal_to_base_n(&self, mut number: u128, length: usize) -> String {
        let mut result = Vec::new();
        while number > 0 {
            result.push(self.reverse_char_map[(number % 62) as usize]);
            number /= 62;
        }

        // Reverse to get the correct order and pad if necessary
        result.reverse();
        let base_n_str: String = result.into_iter().collect();

        // Padding if necessary
        if base_n_str.len() < length {
            return "0".repeat(length - base_n_str.len()) + &base_n_str;
        }

        base_n_str[..length].to_string() // Truncate if longer
    }

    // Generate unique number for alphanumeric string using base-62
    fn generate_unique_number(&self, alphanumeric_str: &str, length: usize) -> String {
        let decimal_value = self.base_n_to_decimal(alphanumeric_str);
        self.decimal_to_base_n(decimal_value, length)
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base62_converter() {
        let converter = Base62Converter::new();

        let alphanumeric_str = "abc123XYZ";
        let length = 12;
        let unique_number = converter.generate_unique_number(alphanumeric_str, length);

        println!("Unique number for '{}': {}", alphanumeric_str, unique_number);
    }
}