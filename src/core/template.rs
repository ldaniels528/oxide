////////////////////////////////////////////////////////////////////
// template module - responsible for matching language tokens
////////////////////////////////////////////////////////////////////

use std::io;

use serde::{Deserialize, Serialize};

use crate::token_slice::TokenSlice;
use crate::tokens::Token;

/// Responsible for matching language tokens to text statements.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Template {
    pub pattern: String,
    pub name: Option<String>,
    #[serde(default)]
    pub is_optional: bool,
    pub children: Option<Vec<Template>>,
}

impl Template {
    ////////////////////////////////////////////////////////////////
    // static methods
    ////////////////////////////////////////////////////////////////
    
    /// Build a template starting with the pattern
    pub fn build(pattern: &str) -> Self {
        Self { pattern: pattern.to_string(), name: None, is_optional: false, children: None }
    }

    /// Retrieves a [Template] via a JSON string
    pub fn from_json(json_str: &str) -> serde_json::Result<Self> {
        serde_json::from_str(json_str)
    }

    /// Creates a new [Template] instance
    pub fn new(pattern: String, name: Option<String>, is_optional: bool, children: Option<Vec<Template>>) -> Self {
        Self { pattern: pattern.to_string(), name, is_optional, children }
    }

    ////////////////////////////////////////////////////////////////
    // instance methods
    ////////////////////////////////////////////////////////////////
    
    pub fn capture(&self, text: &str) -> io::Result<Vec<(String, Token)>> {
        let mut ts = TokenSlice::from_string(text);
        self.capture_all_params(&mut ts)
    }

    /// Extracts and returns values starting at the current position within the slice; which
    /// moves the cursor forward within the slice.
    fn capture_all_params(&self, ts: &mut TokenSlice) -> io::Result<Vec<(String, Token)>> {
        match self.pattern.trim() {
            // is it an atom tag?
            "%a" => self.capture_params(ts),
            // is it a conditional tag?
            "%c" => self.capture_params(ts),
            // is it a expression tag?
            "%e" => self.capture_params(ts),
            // is it a query tag?
            "%q" => self.capture_params(ts),
            // does the token match our pattern?
            s if ts.is(s) => self.capture_params(ts),
            // is it optional?
            _  if self.is_optional => Ok(vec![]),
            // it's required... fail.
            x =>
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Token match error '{}' for '{}'", self.pattern, x),
                ))
        }
    }

    fn capture_params(&self, ts: &mut TokenSlice) -> io::Result<Vec<(String, Token)>> {
        let mut params = vec![];
        // capture the parent if named
        if let Some(name) = &self.name {
            if let Some(parent_token) = ts.current() {
                params.push((name.clone(), parent_token.clone()));
                ts.next();
            }
        }
        // capture the children
        if let Some(children) = &self.children {
            for child in children {
                params.extend(child.capture_all_params(ts)?);
            }
        }
        Ok(params)
    }

    /// Returns a clone of the instance; replacing the `children` attribute.
    pub fn with_children(&self, children: Vec<Template>) -> Self {
        let mut clone = self.clone();
        clone.children = Some(children);
        clone
    }

    /// Returns a clone of the instance; replacing the `is_optional` attribute.
    pub fn with_is_optional(&self, is_optional: bool) -> Self {
        let mut clone = self.clone();
        clone.is_optional = is_optional;
        clone
    }

    /// Returns a clone of the instance; replacing the `name` attribute.
    pub fn with_name(&self, name: &str) -> Self {
        let mut clone = self.clone();
        clone.name = Some(name.into());
        clone
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder() {
        let tpl = Template::build("hello")
            .with_name("greeting")
            .with_is_optional(true)
            .with_children(vec![]);
        assert_eq!(tpl, Template {
            pattern: "hello".into(),
            name: Some("greeting".into()),
            is_optional: true,
            children: Some(vec![]),
        });
    }

    #[test]
    fn test_new_instance() {
        let tpl = Template::new("hello".into(), Some("greeting".into()), true, Some(vec![]));
        assert_eq!(tpl, Template {
            pattern: "hello".into(),
            name: Some("greeting".into()),
            is_optional: true,
            children: Some(vec![]),
        });
    }

    #[test]
    fn test_from_json() {
        // lollypop: select %E:fields ?from +?%q:source +?where +?%c:condition
        let tpl = Template::from_json(r#"{
          "pattern": "select",
          "children": [{
              "pattern": "%E",
              "name": "fields"
            }, {
              "pattern": "from",
              "name": "fields",
              "is_optional": true,
              "children": [{
                  "pattern": "%q",
                  "name": "source"
                }, {
                  "pattern": "where",
                  "is_optional": true,
                  "children": [{
                      "pattern": "%c",
                      "name": "condition"
                    }]
                }]
            }]
        }"#).unwrap();
        assert_eq!(tpl, Template {
            pattern: "select".into(),
            name: None,
            is_optional: false,
            children: Some(vec![
                Template {
                    pattern: "%E".into(),
                    name: Some("fields".into()),
                    is_optional: false,
                    children: None,
                },
                Template {
                    pattern: "from".into(),
                    name: Some("fields".into()),
                    is_optional: true,
                    children: Some(vec![
                        Template {
                            pattern: "%q".into(),
                            name: Some("source".into()),
                            is_optional: false,
                            children: None,
                        },
                        Template {
                            pattern: "where".into(),
                            name: None,
                            is_optional: true,
                            children: Some(vec![
                                Template {
                                    pattern: "%c".into(),
                                    name: Some("condition".into()),
                                    is_optional: false,
                                    children: None,
                                }]),
                        }]),
                }]),
        });
    }

    #[test]
    fn test_extraction() {
        let tpl = Template::from_json(r#"{
          "pattern": "select",
          "name": "command",
          "children": [{
              "pattern": "*",
              "name": "fields"
            }, {
              "pattern": "from",
              "name": "from",
              "is_optional": true,
              "children": [{
                  "pattern": "%q",
                  "name": "table"
                }, {
                  "pattern": "where",
                  "is_optional": true,
                  "children": [{
                      "pattern": "%c",
                      "name": "condition"
                    }]
                }]
            }]
        }"#).unwrap();
        let results = tpl.capture("select * from stocks where symbol is \"AMD\"").unwrap();
        assert_eq!(results, vec![
            ("command".into(), Token::alpha("select".into(), 0, 6, 1, 2)),
            ("fields".into(), Token::operator("*".into(), 7, 8, 1, 9)),
            ("from".into(), Token::alpha("from".into(), 9, 13, 1, 11)),
            ("table".into(), Token::alpha("stocks".into(), 14, 20, 1, 16)),
            ("condition".into(), Token::alpha("where".into(), 21, 26, 1, 23)),
        ]);
    }
}