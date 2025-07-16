#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
//  TestEngine - testing services
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType::{FixedSizeType, NumberType, StringType, TableType};
use crate::dataframe::Dataframe;
use crate::dataframe::Dataframe::TestReport;
use crate::errors::throw;
use crate::errors::Errors::{Exact, TypeMismatch};
use crate::errors::TypeMismatchErrors::StringExpected;
use crate::expression::Conditions::In;
use crate::expression::Expression::{Condition, Feature, Identifier, Literal, Scenario};
use crate::expression::{Conditions, Expression};
use crate::machine::Machine;
use crate::model_row_collection::ModelRowCollection;
use crate::number_kind::NumberKind::I64Kind;
use crate::numbers::Numbers::I64Value;
use crate::parameter::Parameter;
use crate::row_collection::RowCollection;
use crate::structures::Row;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Boolean, Function, Number, StringValue, TableValue, Undefined};
use crate::utils::{pull_name, pull_string_lit};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;

/// Test Error enumerations
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum TestErrors {
    FeatureLevel {
        feat_title: String,
        message: String
    },
    ScenarioLevel {
        feat_title: String,
        scenario_title: String,
        message: String
    }
}

/// Test Status
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TestState {
    name: String,
    errors: Vec<TestErrors>,
    pub scenarios_passed: i64, 
    pub scenarios_failed: i64,
}

impl TestState {
    
    pub fn new(name: String) -> Self {
        Self {
            name,
            scenarios_passed: 0,
            scenarios_failed: 0,
            errors: vec![]
        }
    }
    
    pub fn get_errors(&self) -> &Vec<TestErrors> {
        &self.errors
    }
    
    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn feature_error(
        &mut self,
        feat_title: &String,
        message: String,
    ) {
        self.errors.push(TestErrors::FeatureLevel {
            feat_title: feat_title.clone(),
            message: message.clone()
        });
    }

    pub fn scenario_error(
        &mut self,
        feat_title: &String,
        scenario_title: &String,
        message: String,
    ) {
        self.errors.push(TestErrors::ScenarioLevel {
            feat_title: feat_title.clone(),
            scenario_title: scenario_title.clone(),
            message: message.clone()
        });
    }
    
}

/// Manages and executes test suites
pub struct TestEngine;

impl TestEngine {

    /// feature declaration - unit testing
    /// #### Parameters
    /// - title: the title of the feature
    /// - scenarios: the collection of test scenarios
    /// #### Examples
    /// ```
    /// use testing
    /// feature "Matches function" {
    ///     scenario "Compare Array contents" {
    ///         assert(matches(
    ///             [ 1 "a" "b" "c" ],
    ///             [ 1 "a" "b" "c" ]
    ///         ))
    /// }
    /// ```
    pub fn add_feature(ms: &Machine,
                       title: &Box<Expression>,
                       scenarios: &Vec<Expression>,
    ) -> std::io::Result<(Machine, TypedValue)> {
        let (ms, feature_name) = ms.evaluate(title)?;
        match feature_name {
            StringValue(name) =>
                Ok((ms.with_variable(name.as_str(), Function {
                    params: vec![],
                    body: Feature {
                        title: title.clone(),
                        scenarios: scenarios.clone()
                    }.into(),
                    returns: TableType(vec![])
                }), Boolean(true))),
            other => throw(TypeMismatch(StringExpected(other.to_code())))
        }
    }

    pub fn evaluate_feature(
        ms: &Machine,
        expr: &Expression,
    ) -> std::io::Result<(Machine, TypedValue)> {
        match expr {
            // ex: test report
            Identifier(name) if name == "report" => {
                let (ms, value) = Self::run_features(ms, &Self::find_features(ms, |_| true))?;
                let lines = match value {
                    TableValue(df) => Self::generate_report(df),
                    _ => vec![]
                };
                Ok((ms, StringValue(lines.join("\n"))))
            }
            // ex: test
            Literal(Undefined) =>
                Self::run_features(ms, &Self::find_features(ms, |_| true)),
            // ex: test "Matches function"
            Literal(StringValue(test_name)) =>
                Self::run_features(ms,&Self::find_features(ms, |name| name == test_name)),
            // ex: test "Compare Array contents: Equal" in "Matches function"
            Condition(In(a, b)) => {
                let (scenario_name, feature_name) = (pull_name(a)?, pull_name(b)?);
                Self::run_features(ms, &Self::find_features_in(ms, feature_name.as_str(), scenario_name.as_str()))
            }
            // test where name matches "M*."
            Condition(cond) =>
                Self::run_features(ms,&Self::find_features_where(ms, cond)?),
            // error
            other => throw(Exact(format!("Unrecognized test directive: {}", other.to_code())))
        }
    }

    /// Generates a human-friendly output representing the report.
    pub fn generate_report(input: Dataframe) -> Vec<String> {
        let (horiz, vert, top, middle, bottom) = ('─', '│', '┌', '├', '└');

        fn indent(tabs: i64) -> String {
            let mut out = String::new();
            for _ in 0..tabs { out.push('\t'); }
            out
        }

        let horiz_line = |level: i64, count: usize, edge: char| -> String {
            let mut line = String::new();
            for _ in 0..count { line.push(horiz); }
            format!("{}{}", edge, line)
        };

        let top_line = |level: i64, count: usize| -> String {
            horiz_line(level, count, top)
        };

        let middle_line = |level: i64, count: usize| -> String {
            horiz_line(level, count, middle)
        };

        let bottom_line = |level: i64, count: usize| -> String {
            horiz_line(level, count, bottom)
        };

        // determine how long the separator should be
        let mut separator_len = 20;
        for row in input.iter() {
            let s = row[2].unwrap_value();
            separator_len = separator_len.max(s.len())
        }

        fn icon(level: i64, passed: i64, failed: i64) -> char {
            let (is_header, is_sub_header) = (level == 0, level == 1);
            if is_header {
                match (passed, failed) {
                    (_, 0) => '🟩',
                    (0, _) => '🟥',
                    _ => '🟨'    
                }
            } else if is_sub_header {
                match (passed, failed) {
                    (_, 0) => '🟢',
                    (0, _) => '🔴',
                    _ => '🟡'
                }
            } else {
                if failed == 0 { '✅' } else { '❌' }
            }
        }

        let mut output = vec![];
        output.push(top_line(0, separator_len));
        for row in input.sort_by_columns(&vec![(0, true)]).unwrap_or(input).iter() {
            //println!("{}", row);
            let (level, text, passed, failed) = (row[1].to_i64(), row[2].to_string(), row[3].to_i64(), row[4].to_i64());
            
            // add header upper line?
            let is_header = level == 0;
            if is_header && output.len() > 1 {
                output.push(middle_line(level, separator_len));
            }

            // write the status line
            let line = format!("{}{}{} {}", vert, indent(level), icon(level, passed, failed), text).trim().to_string();
            output.push(line);

            // add header lower line?
            if is_header {
                output.push(middle_line(level, separator_len));
            }
        }
        output.push(bottom_line(0, separator_len));
        output
    }

    pub fn generate_summary(state: &TestState) -> Vec<String> {
        /// Renders test errors to a human-friendly output
        fn render(error: &TestErrors) -> String {
            match error {
                TestErrors::FeatureLevel { feat_title, message } => {
                    format!("❌ [{}] - {}",feat_title, message)
                }
                TestErrors::ScenarioLevel { feat_title, scenario_title, message } => {
                    format!("❌ [{}::{}] - {}", feat_title, scenario_title, message)
                }
            }
        }

        let mut summary = vec![];
        summary.push(format!("📊 {} summary:", state.name));
        summary.push("────────────────────────────────────────────────────────────────────────────────────────".into());
        for error in state.get_errors().iter().map(|te| render(te)).collect::<Vec<_>>() {
            summary.push(format!("{}", error));
        }
        summary.push(format!("✅ {} passed | ❌ {} failed", state.scenarios_passed, state.scenarios_failed));
        summary.push("────────────────────────────────────────────────────────────────────────────────────────".into());

        if state.scenarios_failed > 0 {
            summary.push(format!("👎 {} tests failed. Please review the errors above.", state.scenarios_failed));
        } else {
            summary.push("👍 All tests passed. No issues found.".into());
        }
        summary
    }

    /// feature declaration - unit testing
    /// #### Parameters
    /// - title: the title of the feature
    /// - scenarios: the collection of test scenarios
    /// #### Examples
    /// ```
    /// use testing
    /// feature "Matches function" {
    ///     scenario "Compare Array contents" {
    ///         assert(matches(
    ///             [ "z" "a" "b" "c" ],
    ///             [ "y" "a" "b" "c" ]
    ///         ))
    /// }
    /// ```
    fn do_feature_test(
        ms0: &Machine,
        test_state: &mut TestState,
        report: &mut ModelRowCollection,
        seq: &mut i64,
        title: &Expression,
        scenarios: &Vec<Expression>,
    ) -> std::io::Result<(Machine, ())> {
        let mut scenario_states: HashMap<String, Machine> = HashMap::new();

        /// Captures test outcomes and stores them in the verification report table
        let mut capture = |seq: i64, level: i64, text: String, passed: i64, failed: i64| {
            let row_id = seq as usize;
            report.overwrite_row(row_id, Row::new(row_id, vec![
                Number(I64Value(seq)), Number(I64Value(level)), StringValue(text),
                Number(I64Value(passed)), Number(I64Value(failed)),
            ])).ok();
        };

        /// Returns the next sequence ID
        let mut next_seq = || {
            let my_seq_id = *seq;
            *seq += 1;
            my_seq_id
        };

        // set up the feature details
        let (_ms, feat_title) = ms0.evaluate_as_string(title)?;
        let feat_id = next_seq();

        // start scenario processing
        for operation in scenarios {
            match &operation {
                // scenarios require specialized processing
                Scenario { title, inherits, verifications } => {
                    // get the initial state object
                    let mut mss = match inherits {
                        None => ms0,
                        Some(parent_name) =>
                            match scenario_states.get(parent_name) {
                                None => return throw(Exact(format!("Scenario `{}` was not found", parent_name))),
                                Some(ms1) => ms1
                            }
                    }.clone();

                    // capture the scenario summary ID
                    let (ms1, scenario_title) = mss.evaluate_as_string(title)?;
                    mss = ms1;
                    let scenario_id = next_seq();

                    // verification processing
                    let level = 2;
                    let (mut passed, mut failed) = (0, 0);
                    for verification in verifications {
                        match mss.evaluate(verification) {
                            Ok((msb, _result)) => {
                                mss = msb;
                                capture(next_seq(), level, verification.to_code(), 1, 0);
                                passed += 1;
                            }
                            Err(err) => {
                                capture(next_seq(), level, verification.to_code(), 0, 1);
                                failed += 1;
                                test_state.scenario_error(&feat_title, &scenario_title, err.to_string())
                            }
                        }
                    }

                    // capture the final state of the scenario
                    scenario_states.insert(scenario_title.clone(), mss);

                    // capture the scenario metrics
                    capture(scenario_id, 1, scenario_title, passed, failed);
                    test_state.scenarios_passed += passed;
                    test_state.scenarios_failed += failed;
                }
                // non-scenario execution
                other => match ms0.evaluate(other) {
                    Ok((_ms, _result)) => {
                        capture(next_seq(), 2, other.to_code(), 1, 0);
                        test_state.scenarios_passed += 1;
                    }
                    Err(err) => {
                        capture(next_seq(), 2, other.to_code(), 0, 1);
                        test_state.scenarios_failed += 1;
                        test_state.feature_error(&feat_title, err.to_string())
                    }
                }
            };
        }

        // capture the feature metrics
        capture(feat_id, 0, feat_title, test_state.scenarios_passed, test_state.scenarios_failed);

        // return the report
        Ok((ms0.clone(), ()))
    }

    fn find_features<F>(
        ms: &Machine,
        condition: F,
    ) -> Vec<(Expression, Vec<Expression>)>
    where
        F: Fn(&String) -> bool,
    {
        ms.get_variables().iter()
            .filter_map(|(name, value)|
                if condition(name) {
                    match value {
                        Function { body, .. } => match body.deref() {
                            Feature { title, scenarios } =>
                                Some((title.deref().clone(), scenarios.clone())),
                            _ => None
                        }
                        _ => None
                    }
                } else { None }
            ).collect::<Vec<_>>()
    }

    fn find_features_in(
        ms: &Machine,
        feature_name: &str,
        scenario_name: &str,
    ) -> Vec<(Expression, Vec<Expression>)> {
        Self::find_features(ms, |name| *name == feature_name).iter()
            .map(|(title, scenarios)| {
                let my_scenarios = scenarios.iter()
                    .filter(|s| match s {
                        Scenario { title, .. } =>
                            match title.deref() {
                                Literal(StringValue(title)) => title == scenario_name,
                                _ => false
                            }
                        _ => false
                    }).collect::<Vec<_>>();
                (title.to_owned(), my_scenarios.iter()
                    .map(|e| e.clone().clone()).collect::<Vec<_>>())
            })
            .collect::<Vec<_>>()
    }

    fn find_features_where(
        ms: &Machine,
        condition: &Conditions
    ) -> std::io::Result<Vec<(Expression, Vec<Expression>)>> {
        let mut my_features = vec![];
        for (title, scenarios) in Self::find_features(ms,|_| true).iter() {
            let feature_name = pull_string_lit(title)?;
            let mut my_scenarios = vec![];
            for scenario in scenarios {
                match scenario {
                    Scenario { title, .. } => {
                        let scenario_name = pull_string_lit(title.deref())?;
                        let ms = ms
                            .with_variable("feature_name", StringValue(feature_name.clone()))
                            .with_variable("scenario_name", StringValue(scenario_name));
                        if ms.is_true(condition)?.1  {
                            my_scenarios.push(scenario.to_owned())
                        }
                    }
                    _ => {}
                }
            }

            my_features.push((title.clone(), my_scenarios))
        }
        Ok(my_features)
    }

    fn run_features(
        ms: &Machine,
        features: &Vec<(Expression, Vec<Expression>)>
    ) -> std::io::Result<(Machine, TypedValue)> {
        let mut report = ModelRowCollection::from_parameters(&Self::get_testing_feature_parameters());
        let mut state = TestState::new("Test Suite".into());
        let mut seq = 0;
        for (feat_title, scenarios) in features {
            Self::do_feature_test(ms, &mut state, &mut report, &mut seq, feat_title, scenarios)?;
        }
        
        Ok((ms.to_owned(), TableValue(TestReport(report, state))))
    }

    fn get_testing_feature_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("seq", NumberType(I64Kind)),
            Parameter::new("level", NumberType(I64Kind)),
            Parameter::new("item", FixedSizeType(StringType.into(), 256)),
            Parameter::new("passed", NumberType(I64Kind)),
            Parameter::new("failed", NumberType(I64Kind)),
        ]
    }
    
}

/// Unit tests
#[cfg(test)]
mod tests {
    use crate::testdata::{verify_exact_report, verify_exact_table};

    #[test]
    fn test_fail() {
        verify_exact_report(r#"
            feature "Math tests" {
                scenario "A test that was meant to fail" {
                    assert 5 + 5 == 9
                }
                scenario "Another test that was meant to fail" {
                    assert 2 + 2 == 5
                }
            }
             
            test
        "#, vec![
            "📊 Test Suite summary:",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "❌ [Math tests::A test that was meant to fail] - Assertion failed",
            "❌ [Math tests::Another test that was meant to fail] - Assertion failed",
            "✅ 0 passed | ❌ 2 failed",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "👎 2 tests failed. Please review the errors above.",
            "",
            "┌───────────────────────────────────",
            "│🟥 Math tests",
            "├───────────────────────────────────",
            "│	🔴 A test that was meant to fail",
            "│		❌ assert 5 + 5 == 9",
            "│	🔴 Another test that was meant to fail",
            "│		❌ assert 2 + 2 == 5",
            "└───────────────────────────────────"]);
    }

    #[test]
    fn test_fail_partially() {
        verify_exact_report(r#"
            feature "Array tests" {
                scenario "Compare Array contents: Equal" {
                    assert [ 1 "a" "b" "c" ] matches [ 1 "a" "b" "c" ]
                }
                scenario "Compare Array contents: Not Equal" {
                    assert [ 1 "a" "b" "c" ] matches [ 0 "x" "y" "z" ]
                }
            }
            test
        "#, vec![
            "📊 Test Suite summary:",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "❌ [Array tests::Compare Array contents: Not Equal] - Assertion failed",
            "✅ 1 passed | ❌ 1 failed",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "👎 1 tests failed. Please review the errors above.",
            "",
            "┌─────────────────────────────────────────────────────",
            "│🟨 Array tests",
            "├─────────────────────────────────────────────────────",
            "│	🟢 Compare Array contents: Equal",
            "│		✅ assert [1, 'a', 'b', 'c'] matches [1, 'a', 'b', 'c']",
            "│	🔴 Compare Array contents: Not Equal",
            "│		❌ assert [1, 'a', 'b', 'c'] matches [0, 'x', 'y', 'z']",
            "└─────────────────────────────────────────────────────"]);
    }

    #[test]
    fn test_pass() {
        verify_exact_report(r#"
            feature "JSON tests" {
                scenario "Compare JSON contents (in sequence)" {
                    assert { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
                }
                scenario "Compare JSON contents (out of sequence)" {
                    assert { scores: [82 78 99], id: "A1537" } matches { id: "A1537", scores: [82 78 99] }
                }
            }
            test
        "#, vec![
            "📊 Test Suite summary:",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "✅ 2 passed | ❌ 0 failed",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "👍 All tests passed. No issues found.",
            "",
            "┌───────────────────────────────────────────────────────────────────────────────────────",
            "│🟩 JSON tests",
            "├───────────────────────────────────────────────────────────────────────────────────────",
            "│	🟢 Compare JSON contents (in sequence)",
            "│		✅ assert {first: 'Tom', last: 'Lane'} matches {first: 'Tom', last: 'Lane'}",
            "│	🟢 Compare JSON contents (out of sequence)",
            "│		✅ assert {scores: [82, 78, 99], id: 'A1537'} matches {id: 'A1537', scores: [82, 78, 99]}",
            "└───────────────────────────────────────────────────────────────────────────────────────"]);
    }

    #[test]
    fn test_where() {
        verify_exact_report(r#"
            feature "Array Equality tests" {
                scenario "Compare Array contents" {
                    assert [ 1 "a" "b" "c" ] matches [ 1 "a" "b" "c" ]
                }
            }
            
            feature "JSON Equality tests" {
                scenario "Compare JSON contents (in sequence)" {
                    assert { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
                }
            }
            
            feature "Effective Equality tests" {
              scenario "Compare JSON contents (out of sequence)" {
                    assert { scores: [82 78 99], id: "A1537" } matches { id: "A1537", scores: [82 78 99] }
                }                    
            }
            
            feature "Inequality tests" {
                scenario "Compare Array contents" {
                    assert not [ 1 "a" "b" "c" ] matches [ 0 "x" "y" "z" ]
                }
            }

            test where scenario_name contains "JSON"
        "#, vec![
            "📊 Test Suite summary:", 
            "────────────────────────────────────────────────────────────────────────────────────────", 
            "✅ 2 passed | ❌ 0 failed", 
            "────────────────────────────────────────────────────────────────────────────────────────", 
            "👍 All tests passed. No issues found.", 
            "", 
            "┌───────────────────────────────────────────────────────────────────────────────────────", 
            "│🟩 Array Equality tests", 
            "├───────────────────────────────────────────────────────────────────────────────────────", 
            "├───────────────────────────────────────────────────────────────────────────────────────", 
            "│🟩 JSON Equality tests", 
            "├───────────────────────────────────────────────────────────────────────────────────────", 
            "│	🟢 Compare JSON contents (in sequence)", 
            "│		✅ assert {first: 'Tom', last: 'Lane'} matches {first: 'Tom', last: 'Lane'}", 
            "├───────────────────────────────────────────────────────────────────────────────────────", 
            "│🟩 Effective Equality tests", 
            "├───────────────────────────────────────────────────────────────────────────────────────", 
            "│	🟢 Compare JSON contents (out of sequence)", 
            "│		✅ assert {scores: [82, 78, 99], id: 'A1537'} matches {id: 'A1537', scores: [82, 78, 99]}", 
            "├───────────────────────────────────────────────────────────────────────────────────────", 
            "│🟩 Inequality tests", 
            "├───────────────────────────────────────────────────────────────────────────────────────", 
            "└───────────────────────────────────────────────────────────────────────────────────────"]);
    }

    #[test]
    fn test_report_fail() {
        verify_exact_report(r#"
            feature "Math tests" {
                scenario "A test that was meant to fail" {
                    assert 5 + 5 == 9
                }
                scenario "Another test that was meant to fail" {
                    assert 2 + 2 == 5
                }
            }
             
            test report
        "#, vec![
            "┌───────────────────────────────────",
            "│🟥 Math tests",
            "├───────────────────────────────────",
            "│	🔴 A test that was meant to fail",
            "│		❌ assert 5 + 5 == 9",
            "│	🔴 Another test that was meant to fail",
            "│		❌ assert 2 + 2 == 5",
            "└───────────────────────────────────"]);
    }

    #[test]
    fn test_report_fail_partially() {
        verify_exact_report(r#"
            feature "Array tests" {
                scenario "Compare Array contents: Equal" {
                    assert [ 1 "a" "b" "c" ] matches [ 1 "a" "b" "c" ]
                }
                scenario "Compare Array contents: Not Equal" {
                    assert [ 1 "a" "b" "c" ] matches [ 0 "x" "y" "z" ]
                }
            }
            test report
        "#, vec![
            "┌─────────────────────────────────────────────────────",
            "│🟨 Array tests",
            "├─────────────────────────────────────────────────────",
            "│	🟢 Compare Array contents: Equal",
            "│		✅ assert [1, 'a', 'b', 'c'] matches [1, 'a', 'b', 'c']",
            "│	🔴 Compare Array contents: Not Equal",
            "│		❌ assert [1, 'a', 'b', 'c'] matches [0, 'x', 'y', 'z']",
            "└─────────────────────────────────────────────────────"]);
    }

    #[test]
    fn test_report_pass() {
        verify_exact_report(r#"
            feature "JSON tests" {
                scenario "Compare JSON contents (in sequence)" {
                    assert { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
                }
                scenario "Compare JSON contents (out of sequence)" {
                    assert { scores: [82 78 99], id: "A1537" } matches { id: "A1537", scores: [82 78 99] }
                }
            }
            test report
        "#, vec![
            "┌───────────────────────────────────────────────────────────────────────────────────────",
            "│🟩 JSON tests",
            "├───────────────────────────────────────────────────────────────────────────────────────",
            "│	🟢 Compare JSON contents (in sequence)",
            "│		✅ assert {first: 'Tom', last: 'Lane'} matches {first: 'Tom', last: 'Lane'}",
            "│	🟢 Compare JSON contents (out of sequence)",
            "│		✅ assert {scores: [82, 78, 99], id: 'A1537'} matches {id: 'A1537', scores: [82, 78, 99]}",
            "└───────────────────────────────────────────────────────────────────────────────────────"]);
    }

    #[ignore]
    #[test]
    fn test_features_all() {
        verify_exact_report(r#"
            feature "Array tests" {
                scenario "Compare Array contents: Equal" {
                    assert [ 1 "a" "b" "c" ] matches [ 1 "a" "b" "c" ]
                }
                scenario "Compare Array contents: Not Equal" {
                    assert not [ 1 "a" "b" "c" ] matches [ 0 "x" "y" "z" ]
                }
            }
            
            feature "JSON tests" {
                scenario "Compare JSON contents (in sequence)" {
                    assert { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
                }
                scenario "Compare JSON contents (out of sequence)" {
                    assert { scores: [82 78 99], id: "A1537" } matches { id: "A1537", scores: [82 78 99] }
                }
            }
            
            feature "Math tests" {
                scenario "A test that was meant to fail" {
                    assert 5 + 5 == 9
                }
                scenario "Another test that was meant to fail" {
                    assert 2 + 2 == 5
                }
            }                
             
            test
        "#, vec![
            "📊 Test Suite summary:", 
            "────────────────────────────────────────────────────────────────────────────────────────", 
            "❌ [Math tests::A test that was meant to fail] - Assertion failed", 
            "❌ [Math tests::Another test that was meant to fail] - Assertion failed", 
            "❌ [Array tests::Compare Array contents: Fail Equals] - Assertion failed", 
            "✅ 3 passed | ❌ 3 failed", 
            "────────────────────────────────────────────────────────────────────────────────────────", 
            "👎 3 tests failed. Please review the errors above.", 
            "", 
            "┌───────────────────────────────────────────────────────────────────────────────────────", 
            "│🟥 Math tests", 
            "├───────────────────────────────────────────────────────────────────────────────────────", 
            "│	🔴 A test that was meant to fail", 
            "│		❌ assert 5 + 5 == 9", 
            "│	🔴 Another test that was meant to fail", 
            "│		❌ assert 2 + 2 == 5", 
            "├───────────────────────────────────────────────────────────────────────────────────────", 
            "│🟨 Array tests", 
            "├───────────────────────────────────────────────────────────────────────────────────────", 
            "│	🟢 Compare Array contents: Equals", 
            "│		✅ assert [1, 'a', 'b', 'c'] matches [1, 'a', 'b', 'c']", 
            "│	🔴 Compare Array contents: Fail Equals", 
            "│		❌ assert [1, 'a', 'b', 'c'] matches [0, 'x', 'y', 'z']", 
            "├───────────────────────────────────────────────────────────────────────────────────────", 
            "│🟨 JSON tests", 
            "├───────────────────────────────────────────────────────────────────────────────────────", 
            "│	🟢 Compare JSON contents (in sequence)", 
            "│		✅ assert {first: 'Tom', last: 'Lane'} matches {first: 'Tom', last: 'Lane'}", 
            "│	🟢 Compare JSON contents (out of sequence)", 
            "│		✅ assert {scores: [82, 78, 99], id: 'A1537'} matches {id: 'A1537', scores: [82, 78, 99]}", 
            "└───────────────────────────────────────────────────────────────────────────────────────"]);
    }

    #[test]
    fn test_select_from_where() {
        verify_exact_table(r#"
                feature "Equality tests" {
                    scenario "Compare Array contents" {
                        assert [ 1 "a" "b" "c" ] matches [ 1 "a" "b" "c" ]
                    }
                    scenario "Compare JSON contents (in sequence)" {
                        assert { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
                    }
                }
                
                feature "Effective Equality tests" {
                  scenario "Compare JSON contents (out of sequence)" {
                        assert { scores: [82 78 99], id: "A1537" } matches { id: "A1537", scores: [82 78 99] }
                    }                    
                }
                
                feature "Inequality tests" {
                    scenario "Compare Array contents" {
                        assert not [ 1 "a" "b" "c" ] matches [ 0 "x" "y" "z" ]
                    }
                }
                
                select item, passed, failed
                from
                    (test where scenario_name contains "JSON")
                where level is 2     
                order_by item
            "#, vec![
            r#"|----------------------------------------------------------------------------------------------------------------|"#,
            r#"| id | item                                                                                    | passed | failed |"#,
            r#"|----------------------------------------------------------------------------------------------------------------|"#,
            r#"| 0  | assert {first: "Tom", last: "Lane"} matches {first: "Tom", last: "Lane"}                | 1      | 0      |"#,
            r#"| 1  | assert {scores: [82, 78, 99], id: "A1537"} matches {id: "A1537", scores: [82, 78, 99]}  | 1      | 0      |"#,
            r#"|----------------------------------------------------------------------------------------------------------------|"#]);
    }

    #[test]
    fn test_specific_feature() {
        verify_exact_report(r#"
                feature "Matches via Arrays" {
                    scenario "Compare Array contents: Equal" {
                        assert [ 1 'a' 'b' 'c' ] matches [ 1 'a' 'b' 'c' ]
                    }
                    scenario "Compare Array contents: Not Equal" {
                        assert not [ 1 'a' 'b' 'c' ] matches [ 0 'x' 'y' 'z' ]
                    }
                }
                
                feature "Matches via JSON" {
                    scenario "Compare JSON contents (in sequence)" {
                        assert { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
                    }
                    scenario "Compare JSON contents (out of sequence)" {
                        assert { scores: [82 78 99], id: "A1537" } matches { id: "A1537", scores: [82 78 99] }
                    }
                }

                test "Matches via Arrays"
            "#, vec![
            "📊 Test Suite summary:",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "✅ 2 passed | ❌ 0 failed",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "👍 All tests passed. No issues found.",
            "",
            "┌────────────────────────────────────────────────────────",
            "│🟩 Matches via Arrays",
            "├────────────────────────────────────────────────────────",
            "│	🟢 Compare Array contents: Equal",
            "│		✅ assert [1, 'a', 'b', 'c'] matches [1, 'a', 'b', 'c']",
            "│	🟢 Compare Array contents: Not Equal",
            "│		✅ assert !([1, 'a', 'b', 'c'] matches [0, 'x', 'y', 'z'])",
            "└────────────────────────────────────────────────────────"],
        );
    }

    #[test]
    fn test_specific_scenario_in_feature() {
        verify_exact_report(r#"
                feature "Matches function" {
                    scenario "Compare Array contents: Equal" {
                        assert [ 1 "a" "b" "c" ] matches [ 1 "a" "b" "c" ]
                    }
                    scenario "Compare Array contents: Not Equal" {
                        assert not [ 1 "a" "b" "c" ] matches [ 0 "x" "y" "z" ]
                    }
                    scenario "Compare JSON contents (in sequence)" {
                        assert { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
                    }
                    scenario "Compare JSON contents (out of sequence)" {
                        assert { scores: [82 78 99], id: "A1537" } matches { id: "A1537", scores: [82 78 99] }
                    }
                };
                
                test "Compare Array contents: Not Equal" in "Matches function"
            "#, vec![
            "📊 Test Suite summary:",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "✅ 1 passed | ❌ 0 failed",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "👍 All tests passed. No issues found.",
            "",
            "┌────────────────────────────────────────────────────────",
            "│🟩 Matches function",
            "├────────────────────────────────────────────────────────",
            "│	🟢 Compare Array contents: Not Equal",
            "│		✅ assert !([1, 'a', 'b', 'c'] matches [0, 'x', 'y', 'z'])",
            "└────────────────────────────────────────────────────────"]);
    }

    #[test]
    fn test_scenario_inheritance() {
        verify_exact_report(r#"
            feature "Inheritance test" {
                scenario "Rich Parent" {
                    let count = 5 + 5
                    assert count == 10
                }
                scenario "Greedy Offspring" inherits "Rich Parent" {
                    count += 1
                    assert count == 11
                }
            }
             
            test
        "#, vec![
            "📊 Test Suite summary:",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "✅ 4 passed | ❌ 0 failed",
            "────────────────────────────────────────────────────────────────────────────────────────",
            "👍 All tests passed. No issues found.",
            "",
            "┌────────────────────",
            "│🟩 Inheritance test",
            "├────────────────────",
            "│	🟢 Rich Parent",
            "│		✅ count = 5 + 5",
            "│		✅ assert count == 10",
            "│	🟢 Greedy Offspring",
            "│		✅ count = count + 1",
            "│		✅ assert count == 11",
            "└────────────────────"]);
    }

    #[test]
    fn test_scenario_standalone() {
        verify_exact_report(r#"
            scenario "Compare Array contents: Equal" {
                assert [ 1 "a" "b" "c" ] matches [ 1 "a" "b" "c" ]
            }
            test
        "#, vec![
            "📊 Test Suite summary:", 
            "────────────────────────────────────────────────────────────────────────────────────────", 
            "✅ 1 passed | ❌ 0 failed", 
            "────────────────────────────────────────────────────────────────────────────────────────", 
            "👍 All tests passed. No issues found.", 
            "", 
            "┌─────────────────────────────────────────────────────", 
            "│🟩 Compare Array contents: Equal", 
            "├─────────────────────────────────────────────────────", 
            "│	🟢 Compare Array contents: Equal", 
            "│		✅ assert [1, 'a', 'b', 'c'] matches [1, 'a', 'b', 'c']", 
            "└─────────────────────────────────────────────────────"]
        );
    }
}