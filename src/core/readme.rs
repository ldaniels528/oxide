#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Oxide README.md Generation
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType;
use crate::expression::Expression::{DoWhile, While};
use crate::expression::Queryables::Where;
use crate::expression::Ranges::Exclusive;
use crate::expression::{Conditions, DatabaseOps, Expression, HttpMethodCalls};
use crate::interpreter::Interpreter;
use crate::platform::{Package, PackageOps};
use crate::row_collection::RowCollection;
use crate::structures::Structure;
use crate::table_renderer::TableRenderer;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{NamespaceValue, Structured, TableValue};
use crate::utils::{strip_margin, superscript};
use log::__private_api::Value;
use shared_lib::cnv_error;
use std::fs::{File, OpenOptions};
use std::io::Write;

fn generate_readme(file: File) -> std::io::Result<File> {
    println!("generate title...");
    let mut file = generate_title(file)?;

    // println!("generate development...");
    // let file = generate_development(file)?;

    // println!("generate run tests...");
    // let mut file = generate_run_tests(file)?;

    println!("generate language examples...");
    let lang_header = strip_margin(r#"
        |<a name="examples"></a>
        |### 📖 Core Language Examples
    "#, '|');
    writeln!(file, "{lang_header}")?;
    let mut file = generate_language_examples(file)?;

    println!("generate platform examples...");
    let plat_header = strip_margin(r#"
        |<a name="platform_examples"></a>
        |### 📦 Platform Examples
    "#, '|');
    writeln!(file, "{plat_header}")?;
    let file = generate_platform_examples(file)?;

    // println!("generate rpc...");
    // let file = generate_rpc(file)?;
    Ok(file)
}

fn generate_title(mut file: File) -> std::io::Result<File> {
    file.write(r#"
🧪 Oxide — A Lightweight, Modern Language for Data, APIs & Automation
========================================================================

**Oxide** is a clean, expressive scripting language built for the modern developer. Whether you're transforming data, automating workflows, building APIs, or exploring time-based events, Oxide empowers you with elegant syntax and a practical standard library—designed to make complex operations feel intuitive.

---

## 🚀 Why Choose Oxide?

## ✅ **Clean, Functional Syntax**
Write less, do more. Concise expressions, intuitive chaining, and minimal boilerplate make Oxide a joy to use.

## 🧰 **Batteries Included**
Built-in modules like `cal`, `io`, `math`, `str`, `www`, and more cover the essentials—without reaching for external libraries.

## 🔗 **Composable Pipelines**
Use `:::` to build seamless transformation pipelines—perfect for chaining, mapping, filtering, and data shaping.

## 🌐 **Web-Native by Design**
Call an API, parse the response, and persist results—in a single line of code.

## 🧠 **Human-Centered**
Inspired by functional programming, Oxide is readable, predictable, and powerful enough for real-world use without excess noise.

---

## 🧰 What Can You Do with Oxide?

### 🌍 Call APIs and Handle Responses
```oxide
GET https://api.example.com/users
```

### 🧮 Transform Arrays and Maps
```oxide
use arrays
users = [ { name: 'Tom' }, { name: 'Sara' } ]
names = users:::map(u -> u::name)
```

### 🕒 Work with Dates and Durations
```oxide
use cal, durations
cal::plus(now(), 30:::days())
```

### 🔄 Compose Data Pipelines
```oxide
use arrays
let arr = [1, 2, 3, 4]
(arr:::filter(x -> (x % 2) == 0)):::map(x -> x * 10)
```

---

## 👥 Who Is Oxide For?

- **Data Engineers & Analysts** — quick scripting for time and table-based operations.
- **Web Developers** — seamless API interactions and response transformations.
- **Scripters & Hackers** — ideal for automation, file operations, and glue code.
- **Language Enthusiasts** — a functional-style pipeline DSL with just enough structure.

---

## 🛠️ Getting Started

### 🔧 Build the REPL & Server

```bash
cargo build --release
```

Artifacts will be in `./target/release/`:
- `oxide` – Oxide REPL / Server

### ✅ Run the Tests

```bash
cargo test
```

> 🔬 Over 800 tests (and counting) ensure Oxide's reliability and edge-case coverage.

---

## 📦 Core Modules & Platform

The remainder of this document showcases categorized usage examples across Oxide's standard modules including:

- `arrays`, `cal`, `durations`, `io`, `math`, `os`, `oxide`, `str`, `tools`, `util`, `www`, and `testing`.

To improve navigation, consider splitting the examples into separate markdown files or auto-generating docs from code annotations using a tool like `mdBook`, `Docusaurus`, or a custom Rust doc generator.
"#.as_bytes())?;
    Ok(file)
}

fn generate_development(mut file: File) -> std::io::Result<File> {
    file.write(r#"
## Development

#### Build the Oxide REPL and Server

```bash
cargo build --release
```

You'll find the executables in `./target/release/`:
* `oxide_repl` is the Oxide REST client / REPL
* `oxide_server` is the Oxide REST Server
"#.as_bytes())?;
    Ok(file)
}

fn generate_language_examples(mut file: File) -> std::io::Result<File> {
    for (name, examples) in create_language_examples() {
        println!("[+] {}", name);
        
        // header section
        // ex: "oxide::version - ..."
        writeln!(file, "<hr>")?;
        writeln!(file, "<h4>▶️ {}</h4>", name)?;

        // write the example bodies
        for (n, example) in examples.iter().enumerate() {
            let example_body = format!("<pre>{}</pre>", example.trim());
            writeln!(file, "<h5>example{}</h5>", superscript(n + 1))?;
            writeln!(file, "{}", example_body)?;

            // write the results body
            writeln!(file, "<h5>results</h5>")?;
            match generate_language_results(example.as_str()) {
                Ok(out_lines) => file = print_text_block(file, out_lines)?,
                Err(err) => {
                    println!("{}", example);
                    println!("ERROR: {}", err.to_string());
                    Err(err.to_string()).unwrap()
                }
            }
        }
    }
    Ok(file)
}

fn generate_platform_examples(mut file: File) -> std::io::Result<File> {
    for op in PackageOps::get_contents() {
        for (n, example) in op.get_examples().iter().enumerate() {
            if !example.is_empty() {
                println!("[+] {}::{}", op.get_package_name(), op.get_name());

                // header section
                // ex: "oxide::version - ..."
                writeln!(file, "<hr>")?;
                writeln!(file, "<h4>📦 {}::{} &#8212; {}</h4>",
                         op.get_package_name(), op.get_name(), op.get_description())?;

                // write the example body
                writeln!(file, "<h5>example{}</h5>", n + 1)?;
                let example_body = format!("<pre>{}</pre>", example.trim());
                writeln!(file, "{}", example_body)?;

                // write the results body
                writeln!(file, "<h5>results</h5>")?;
                let out_lines = generate_example_results(example)?;
                file = print_text_block(file, out_lines)?
            }
        }
    }
    Ok(file)
}

fn generate_language_results(example: &str) -> std::io::Result<Vec<String>> {
    let value = Interpreter::new().evaluate(example)?;
    match value {
        TableValue(df) => {
            let rc: Box<dyn RowCollection> = Box::new(df);
            TableRenderer::from_table_with_ids(&rc)
        }
        other => Ok(vec![other.to_code()]),
    }
}

fn generate_example_results(example: &str) -> std::io::Result<Vec<String>> {
    match Interpreter::new().evaluate(example)? {
        NamespaceValue(ns) => {
            let df = ns.load_table()?;
            Ok(TableRenderer::from_dataframe(&df))
        }
        Structured(s) => {
            Ok(vec![
                format!(r#"
                    ```json
                    {}
                    ```
                "#, s.to_pretty_json()?)
            ])
        }
        TableValue(df) => {
            let rc: Box<dyn RowCollection> = Box::new(df);
            TableRenderer::from_table_with_ids(&rc)
        }
        other => {
            //println!("readme: other {:?}", other);
            Ok(vec![other.display_value()])
        }
    }
}

fn format_as_json(value: TypedValue) -> std::io::Result<String> {
    let raw = value.unwrap_value();
    let parsed: Value = serde_json::from_str(raw.as_str())?;
    serde_json::to_string_pretty(&parsed).map_err(|e| cnv_error!(e))
}

fn generate_run_tests(mut file: File) -> std::io::Result<File> {
    file.write(r#"
#### Run the tests

To run the tests (~ 840 tests at the time of writing):

```bash
cargo test
```
    "#.as_bytes())?;
    Ok(file)
}

fn generate_rpc(mut file: File) -> std::io::Result<File> {
    file.write(r#"
<a name="rpc"></a>
#### Remote Procedure Calls

Remote Procedure Call (RPC) is a feature that allows Oxide to evaluate expressions across
remote peers.

```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{
          "code": "5 + 5"
         }' \
     http://0.0.0.0:8080/rpc
```

server response:

```json
10.0
```
    "#.as_bytes())?;
    Ok(file)
}

pub fn get_language_examples(model: &Expression) -> Vec<String> {
    match model {
        Expression::ArrayExpression(..) => vec![
            strip_margin(r#"
                |// Arrays can be defined via ranges
                |
                |1..7
            "#, '|'),
            strip_margin(r#"
                |// Arrays can be created using literals
                |
                |[1, 4, 2, 8, 5, 7]
            "#, '|'),
            strip_margin(r#"
                |// Arrays may be destructured to assign multiple variables
                |
                |let [a, b, c] = [3, 5, 7]
                |a + b + c
            "#, '|'),
            strip_margin(r#"
                |// Arrays can be transformed via the 'arrays' package
                |
                |arrays::reverse([1, 4, 2, 8, 5, 7])
            "#, '|')
        ],
        Expression::BitwiseAnd(..) => vec!["0b1111 & 0b0101".into()],
        Expression::BitwiseOr(..) => vec!["0b1010 | 0b0101".into()],
        Expression::BitwiseShiftLeft(..) => vec!["20 << 3".into()],
        Expression::BitwiseShiftRight(..) => vec!["20 >> 3".into()],
        Expression::BitwiseXor(..) => vec!["0b1111 ^ 0b0101".into()],
        Expression::Coalesce(..) => vec![],
        Expression::CodeBlock(..) => vec![
            strip_margin(r#"
                |result = {
                |    let (a, b) = (5, 9)
                |    a + b
                |}
                |result
            "#, '|')],
        Expression::ColonColon(..) => vec![
            strip_margin(r#"
                |tools::to_table([
                |    'apple', 'berry', 'kiwi', 'lime'
                |])
            "#, '|')],
        Expression::ColonColonColon(..) => vec![
            strip_margin(r#"
                |use durations
                |8:::hours()
            "#, '|')],
        Expression::Condition(..) => vec![
            strip_margin(r#"
                    |let x = 10
                    |x in 5..=10
                "#, '|'),
            strip_margin(r#"
                    |let x = 10
                    |x in 5..10
                "#, '|'),
            strip_margin(r#"
                    |let x = 1..8
                    |x contains 7
                "#, '|'),
        ],
        Expression::CurvyArrowLeft(..) => vec![
            strip_margin(r#"
                |stocks = ns("expressions.read_next_row.stocks")
                |table(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: Date)) ~> stocks
                |rows = [{ symbol: "BIZ", exchange: "NYSE" }, { symbol: "GOTO", exchange: "OTC" }]
                |rows ~> stocks
                |// read the last row
                |last_row <~ stocks
                |last_row
            "#, '|')],
        Expression::CurvyArrowRight(..) => vec![
            strip_margin(r#"
                |stocks = ns("expressions.into.stocks")
                |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                |rows = [
                |   { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                |   { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                |   { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                |]
                |rows ~> stocks
            "#, '|')],
        Expression::DatabaseOp(..) => vec![],
        Expression::Divide(..) => vec![
            "20.0 / 3".into(),
        ],
        Expression::DoWhile { .. } => vec![
            strip_margin(r#"
                |let i = 0
                |do {
                |    i = i + 1
                |    yield i * 2
                |} while (i < 5)
            "#, '|')
        ],
        Expression::ElementAt(..) => vec![
            strip_margin(r#"
                |let arr = [1, 4, 2, 8, 5, 7]
                |arr[3]
            "#, '|')],
        Expression::Feature { .. } => vec![
            strip_margin(r#"
                |use testing
                |Feature "Matches function" {
                |    Scenario "Compare Array contents: Equal" {
                |        assert(
                |            [ 1 "a" "b" "c" ] matches [ 1 "a" "b" "c" ]
                |        )
                |    }
                |    Scenario "Compare Array contents: Not Equal" {
                |        assert(!(
                |            [ 1 "a" "b" "c" ] matches [ 0 "x" "y" "z" ]
                |        ))
                |    }
                |    Scenario "Compare JSON contents (in sequence)" {
                |        assert(
                |           { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
                |        )
                |    }
                |    Scenario "Compare JSON contents (out of sequence)" {
                |        assert(
                |           { scores: [82 78 99], id: "A1537" } 
                |                       matches 
                |           { id: "A1537", scores: [82 78 99] }
                |        )
                |    }
                |}"#, '|')],
        Expression::FnExpression { .. } => vec![
            strip_margin(r#"
                |product = (a, b) -> a * b
                |product(2, 5)
            "#, '|')],
        Expression::For { .. } => vec![
            strip_margin(r#"
                |for row in tools::to_table(['apple', 'berry', 'kiwi', 'lime']) 
                |    yield row::value
            "#, '|')],
        Expression::From(..) => vec![
            strip_margin(r#"
                |stocks = tools::to_table([
                |   { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                |   { symbol: "GRU", exchange: "NYSE", last_sale: 56.88 },
                |   { symbol: "APK", exchange: "NASDAQ", last_sale: 32.12 }
                |])
                |from stocks where last_sale > 20.0
            "#, '|')],
        Expression::FunctionCall { .. } => vec![],
        Expression::HTTP(..) => vec![
            strip_margin(r#"
                    |stocks = ns("readme.www.stocks")
                    |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                    |www::serve(8833)
                    "#, '|'),
            strip_margin(r#"
                    |POST {
                    |    url: http://localhost:8833/platform/www/stocks/0
                    |    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }
                    |}
                    "#, '|'),
            "GET http://localhost:8833/platform/www/stocks/0".into(),
            "HEAD http://localhost:8833/platform/www/stocks/0".into(),
            strip_margin(r#"
                    |PUT {
                    |    url: http://localhost:8833/platform/www/stocks/0
                    |    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.79 }
                    |}
                    "#, '|'),
            "GET http://localhost:8833/platform/www/stocks/0".into(),
            strip_margin(r#"
                    |PATCH {
                    |    url: http://localhost:8833/platform/www/stocks/0
                    |    body: { last_sale: 11.81 }
                    |}
                    "#, '|'),
            "GET http://localhost:8833/platform/www/stocks/0".into(),
            "DELETE http://localhost:8833/platform/www/stocks/0".into(),
            "GET http://localhost:8833/platform/www/stocks/0".into(),
        ],
        Expression::If { .. } => vec![
            strip_margin(r#"
                    |// Oxide provides an if-else statement
                    |
                    |let x = 4
                    |if(x > 5) "Yes"
                    |else if(x < 5) "Maybe"
                    |else "No"
                    "#, '|'),
            strip_margin(r#"
                    |// Oxide also provides iff - a ternary-operator-like if function
                    |
                    |fact = n -> iff(n <= 1, 1, n * fact(n - 1))
                    |fact(6)
                    "#, '|'),
        ],
        Expression::Include(..) => vec![],
        Expression::KeyValue(..) => vec![],
        Expression::Literal(..) => vec![],
        Expression::MatchExpression(..) => vec![
            strip_margin(r#"
                |let code = 103
                |match code [
                |   n: 100 => "Accepted",
                |   n: 101..104 => 'Escalated',
                |   n: n > 0 && n < 100 => "Pending",
                |   n => "Rejected: code {n}"
                |]
            "#, '|')
        ],
        Expression::Minus(..) => vec![
            "188 - 36".into(),
        ],
        Expression::Module(..) => vec![],
        Expression::Modulo(..) => vec![],
        Expression::Multiply(..) => vec![
            "5 * 6".into()
        ],
        Expression::NamedValue(..) => vec![
            "name: 'Tom'".into(),
            "from { name: 'Tom' }".into()
        ],
        Expression::Neg(..) => vec![
            strip_margin(r#"
                |let i = 75
                |let j = -i
                |j
            "#, '|')],
        Expression::New(..) => vec![
            "new Table(symbol: String(8), exchange: String(8), last_sale: f64)".into()
        ],
        Expression::Ns(..) => vec![],
        Expression::Parameters(..) => vec![],
        Expression::Plus(..) => vec![
            "5 + 6".into()
        ],
        Expression::PlusPlus(..) => vec![],
        Expression::Pow(..) => vec![
            "2 ** 3".into()
        ],
        Expression::Range(..) => vec![
            strip_margin(r#"
                |// Ranges may be exclusive
                |
                |range = 1..5
                |tools::reverse(range)
            "#, '|'),
        strip_margin(r#"
                |// Ranges may be inclusive
                |
                |range = 1..=5
                |tools::reverse(range)
            "#, '|')
        ],
        Expression::Return(..) => vec![],
        Expression::Scenario { .. } => vec![],
        Expression::SetVariables(..) => vec![
            strip_margin(r#"
                |let a = 3
                |let b = 5
                |let c = 7
                |a + b + c
            "#, '|'),
        ],
        Expression::SetVariablesExpr(..) => vec![
            strip_margin(r#"
                |// Use ":=" to simultaneously assign a value and return the assigned value
                |
                |let i = 0
                |while (i < 5) yield (i := i + 1) * 3
            "#, '|'),
        ],
        Expression::StructureExpression(..) => vec![],
        Expression::TupleExpression(..) => vec![
            strip_margin(r#"
                |// Tuples may be destructured to assign multiple variables
                |
                |(a, b, c) = (3, 5, 7)
                |a + b + c
            "#, '|'),
            strip_margin(r#"
                |// Tuples support addition
                |
                |let a = (2, 4, 6)
                |let b = (1, 2, 3)
                |a + b
            "#, '|'),
            strip_margin(r#"
                |// Tuples support subtraction
                |
                |let a = (3, 5, 7)
                |let b = (1, 0, 1)
                |a - b
            "#, '|'),
            strip_margin(r#"
                |// Tuples support negation
                |
                |-(3, 6, 9)
            "#, '|'),
            strip_margin(r#"
                |// Tuples support multiplication
                |
                |let a = (3, 5, 7)
                |let b = (1, 0, 1)
                |a * b
            "#, '|'),
            strip_margin(r#"
                |// Tuples support division
                |
                |let a = (3.0, 5.0, 9.0)
                |let b = (1.0, 2.0, 1.0)
                |a / b
                "#, '|'),
            strip_margin(r#"
                |// Tuples support modulus
                |
                |let a = (3.0, 5.0, 9.0)
                |let b = (1.0, 2.0, 1.0)
                |a % b
                "#, '|'),
            strip_margin(r#"
                |// Tuples support exponents
                |
                |let a = (2, 4, 6)
                |let b = (1, 2, 3)
                |a ** b
            "#, '|'),
        ],
        Expression::TypeDef(..) => vec![
            strip_margin(r#"
                |LabelString = typedef(String(80))
                |LabelString
            "#, '|')
        ],
        Expression::Use(..) => vec![
            strip_margin(r#"
                |use tools
                |stocks = to_table([
                |   { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                |   { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                |   { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                |])
                |stocks
            "#, '|')
        ],
        Expression::Variable(..) => vec![
            strip_margin(r#"
                |let (a, b, c) = (3, 5, 7)
                |c > b
            "#, '|')
        ],
        Expression::VerticalBarArrow(..) => vec![],
        Expression::VerticalBarDoubleArrow(..) => vec![
            strip_margin(r#"
               |use tools::reverse
               |result = 'Hello' |> reverse
               |result
               "#, '|'),
            strip_margin(r#"
               |// arrays, tuples and structures can be deconstructed into arguments
               |
               |fn add(a, b) -> a + b
               |fn inverse(a) -> 1.0 / a
               |result = ((2, 3) |>> add) |> inverse
               |result
               "#, '|')
        ],
        Expression::Via(..) => vec![
            strip_margin(r#"
                |stocks = ns("readme.via.stocks")
                |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                |
                |rows = [
                |   { symbol: "ABCQ", exchange: "AMEX", last_sale: 12.49 },
                |   { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                |   { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                |]
                |rows ~> stocks
                |
                |overwrite stocks via {symbol: "ABC", exchange: "NYSE", last_sale: 0.2308}
                |where symbol is "ABCQ"
                |
                |from stocks
            "#, '|')
        ],
        Expression::When { .. } => vec![
            strip_margin(r#"
                |// Executes the block at the moment the condition becomes true.
                |let (x, y) = (1, 0)
                |when x == 0 {
                |    x = x + 1
                |    y = y + 1
                |}
                |x = x - 1
                |x + y
            "#, '|'),
                        strip_margin(r#"
                |// The block will not be executed if the condition is already true.
                |let (x, y) = (1, 0)
                |when x == 0 || y == 0 {
                |    x = x + 1
                |    y = y + 1
                |}
                |x + y
            "#, '|'),
            strip_margin(r#"
                |// The block will be executed after the second assignment.
                |let (x, y) = (1, 0)
                |when x == 0 || y == 0 {
                |    x = x + 1
                |    y = y + 1
                |}
                |let (x, y) = (2, 3)
                |x + y
            "#, '|'),
        ],
        Expression::While { .. } => vec![
            strip_margin(r#"
                |let x = 0
                |while (x < 5) {
                |    x = x + 1
                |    yield x * 2
                |}
            "#, '|')
        ],
        Expression::Yield(..) => vec![
            strip_margin(r#"
                for(i = 0, i < 5, i = i + 1) yield i * 2
            "#, '|')
        ],
    }
}

fn create_language_examples() -> Vec<(String, Vec<String>)> {
    use crate::expression::Expression::*;
    let null = Box::new(Literal(TypedValue::Null));
    let models = vec![
        ("Arrays", ArrayExpression(vec![])),
        ("Aliases", NamedValue("".into(), null.clone())),
        ("Bitwise And", BitwiseAnd(null.clone(), null.clone())),
        ("Bitwise Or", BitwiseOr(null.clone(), null.clone())),
        ("Bitwise Shift-Left", BitwiseShiftLeft(null.clone(), null.clone())),
        ("Bitwise Shift-Right", BitwiseShiftRight(null.clone(), null.clone())),
        ("Bitwise XOR", BitwiseXor(null.clone(), null.clone())),
        ("Code Block", CodeBlock(vec![])),
        ("Method Call", ColonColon(null.clone(), null.clone())),
        ("Implicit Method Call", ColonColonColon(null.clone(), null.clone())),
        ("Conditionals", Condition(Conditions::True)),
        ("Curvy-Arrow Left", CurvyArrowLeft(null.clone(), null.clone())),
        ("Curvy-Arrow Right", CurvyArrowRight(null.clone(), null.clone())),
        ("SQL", DatabaseOp(DatabaseOps::Queryable(Where { from: null.clone(), condition: Conditions::True }))),
        ("Mathematics: division", Divide(null.clone(), null.clone())),
        ("Do-While expression", DoWhile { condition: null.clone(), code: null.clone() }),
        ("Arrays: Indexing", ElementAt(null.clone(), null.clone())),
        ("Testing", Feature { title: null.clone(), scenarios: vec![] }),
        ("Functions", FnExpression { params: vec![], body: None, returns: DataType::BooleanType }),
        ("Function-Call", FunctionCall { fx: null.clone(), args: vec![] }),
        ("Iteration", For { construct: null.clone(), op: null.clone() }),
        ("Query", From(null.clone())),
        ("HTTP", HTTP(HttpMethodCalls::GET(null.clone()))),
        ("IF expression", If { condition: null.clone(), a: null.clone(), b: None }),
        ("Includes", Include(null.clone())),
        //("Match expression", MatchExpression(null.clone(), vec![])),
        ("Mathematics: subtraction", Minus(null.clone(), null.clone())),
        ("Mathematics: multiplication", Multiply(null.clone(), null.clone())),
        ("Negative", Neg(null.clone())),
        ("Mathematics: addition", Plus(null.clone(), null.clone())),
        ("New Instances", New(null.clone())),
        ("Ranges", Range(Exclusive(null.clone(), null.clone()))),
        ("Assignment (statement)", SetVariables(null.clone(), null.clone())),
        ("Assignment (expression)", SetVariablesExpr(null.clone(), null.clone())),
        ("Structures", StructureExpression(vec![])),
        ("Tuples", TupleExpression(vec![])),
        ("Type Definitions", TypeDef(null.clone())),
        ("Function Pipelines", VerticalBarArrow(null.clone(), null.clone())),
        ("Function Pipelines (destructuring)", VerticalBarDoubleArrow(null.clone(), null.clone())),
        ("Import/Use", Use(vec![])),
        ("Via Clause", Via(null.clone())),
        ("When statement", When { condition: null.clone(), code: null.clone() }),
        ("While expression", While { condition: null.clone(), code: null.clone() }),
        ("Yield", Yield(null.clone())),
    ];

    let mut examples = models.iter()
        .map(|(title, model)| (title.to_string(), get_language_examples(&model)))
        .filter(|(_, examples)| !examples.is_empty())
        .collect::<Vec<_>>();
    examples.sort_by(|a, b| a.0.cmp(&b.0));
    examples
}

fn print_text_block(mut file: File, lines: Vec<String>) -> std::io::Result<File> {
    writeln!(file, "<pre>")?;
    for line in lines {
        writeln!(file, "{}", line)?;
    }
    writeln!(file, "</pre>")?;
    Ok(file)
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    #[test]
    fn test_language_examples() {
        for (name, examples) in create_language_examples() {
            println!("[+] {}", name);

            for example in examples {
                println!("    {}", example);
                generate_language_results(example.as_str()).unwrap();
            }
        }
    }

    #[ignore]
    #[test]
    fn test_language_example_generation() {
        let mut file = OpenOptions::new()
            .truncate(true).create(true).read(true).write(true)
            .open("../../language.md")
            .unwrap();
        writeln!(file, "📖 Core Language Examples").unwrap();
        writeln!(file, "========================================").unwrap();
        file = generate_language_examples(file).unwrap();
        file.flush().unwrap();
    }

    #[ignore]
    #[test]
    fn test_platform_example_generation() {
        let mut file = OpenOptions::new()
            .truncate(true).create(true).read(true).write(true)
            .open("../../platform.md")
            .unwrap();
        writeln!(file, "📦 Platform Examples").unwrap();
        writeln!(file, "========================================").unwrap();
        file = generate_platform_examples(file).unwrap();
        file.flush().unwrap();
    }

    #[ignore]
    #[test]
    fn test_generate_readme() {
        let file = OpenOptions::new()
            .truncate(true).create(true).read(true).write(true)
            .open("../../README.md")
            .unwrap();
        match generate_readme(file) {
            Ok(mut file) => file.flush().unwrap(),
            Err(err) => {
                println!("{}", err);
                Err(err.to_string()).unwrap()
            }
        }
    }

    #[test]
    fn test_generate_docs() {
        test_language_example_generation();
        test_platform_example_generation();
        test_generate_readme();
    }
}