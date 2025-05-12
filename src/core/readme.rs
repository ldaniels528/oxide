#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Oxide README.md Generation
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType;
use crate::expression::{Conditions, DatabaseOps, Directives, Expression, HttpMethodCalls};
use crate::interpreter::Interpreter;
use crate::platform::{PlatformOps, PLATFORM_OPCODES};
use crate::row_collection::RowCollection;
use crate::table_renderer::TableRenderer;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::TableValue;
use shared_lib::strip_margin;
use std::fs::{File, OpenOptions};
use std::io::Write;
use crate::expression::Queryables::Where;

fn generate_readme(mut file: File) -> std::io::Result<File> {
    println!("generate title...");
    let file = generate_title(file)?;

    println!("generate project status...");
    let file = generate_project_status(file)?;

    println!("generate development...");
    let file = generate_development(file)?;

    println!("generate run tests...");
    let file = generate_run_tests(file)?;

    println!("generate getting started...");
    let file = generate_getting_started(file)?;

    println!("generate language examples...");
    let file = generate_language_examples(file)?;

    println!("generate platform examples...");
    let file = generate_platform_examples(file)?;

    println!("generate rpc...");
    let file = generate_rpc(file)?;
    Ok(file)
}

fn generate_title(mut file: File) -> std::io::Result<File> {
    file.write(r#"
Oxide
=====

## Motivation

The purpose of this project is to create a development platform for small to medium software projects
and proof of concept software projects. The system will offer:
* Rust-inspired language syntax
* integrated dataframes with SQL-like grammar for queries
* integrated REST webservices
* integrated testing framework

Oxide is the spiritual successor to [Lollypop](https://github.com/ldaniels528/lollypop), a multi-paradigm language also
featuring integrated dataframes with SQL-like grammar for queries, but built for the JVM and
developed in the Scala programming language.
"#.as_bytes())?;
    Ok(file)
}

fn generate_project_status(mut file: File) -> std::io::Result<File> {
    file.write(r#"
## Project Status

- The <a href='#REPL'>REPL</a> is now available, and allows you to issue commands directly to the server.
- The database server is also now available and supports basic CRUD operations via REST for:
  - <a href='#create_table'>creating tables</a>
  - <a href='#drop_table'>dropping tables</a>
  - <a href='#overwrite_row'>insert/overwrite a row by offset</a>
  - <a href='#read_row'>retrieve a row by offset</a>
  - <a href='#delete_row'>delete a row by offset</a>
  - <a href='#rpc'>remote procedure calls</a>
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
    writeln!(file, "<a name=\"examples\"></a>\n#### Basic Syntax")?;
    for (name, examples) in get_language_examples() {
        // header section
        // ex: "oxide::version - ..."
        writeln!(file, "<hr>")?;
        writeln!(file, "<h4>{}</h4>", name)?;

        // write the example bodies
        for example in examples {
            let example_body = format!("<pre>{}</pre>", example.trim());
            writeln!(file, "{}", example_body)?;

            // write the results body
            match generate_language_results(example.as_str()) {
                Ok(out_lines) => file = print_text_block(file, out_lines)?,
                Err(err) => {
                    writeln!(file, "ERROR: {}", err.to_string())?;
                }
            }
        }
    }
    Ok(file)
}

fn generate_platform_examples(mut file: File) -> std::io::Result<File> {
    writeln!(file, r#"
<a name="examples"></a>
#### Platform Examples
"#)?;

    for op in PLATFORM_OPCODES {
        let example = op.get_example();
        if !example.is_empty() {
            // header section
            // ex: "oxide::version - ..."
            writeln!(file, "<hr>")?;
            writeln!(file, "<h4>{}::{} &#8212; {}</h4>",
                     op.get_package_name(), op.get_name(), op.get_description())?;

            // write the example body
            let example_body = format!("<pre>{}</pre>", op.get_example().trim());
            writeln!(file, "{}", example_body)?;

            // write the results body
            match generate_example_results(op) {
                Ok(out_lines) => file = print_text_block(file, out_lines)?,
                Err(err) => {
                    writeln!(file, "ERROR: {}", err.to_string())?;
                }
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
        other => Ok(vec![other.unwrap_value()]),
    }
}

fn generate_example_results(op: PlatformOps) -> std::io::Result<Vec<String>> {
    let value = Interpreter::new().evaluate(op.get_example().as_str())?;
    match value.to_table_or_value() {
        TableValue(df) => {
            let rc: Box<dyn RowCollection> = Box::new(df);
            TableRenderer::from_table_with_ids(&rc)
        }
        other => Ok(vec![other.unwrap_value()]),
    }
}

fn generate_getting_started(mut file: File) -> std::io::Result<File> {
    file.write(r#"
## Getting Started

<a name="REPL"></a>
### REPL

The Oxide REPL is now available, and with it, you can issue commands directly to the server.
Oxide can evaluate basic expressions:

```bash
$ oxide
Welcome to Oxide REPL. Enter "q!" to quit.

oxide.public[0]> 5 + 9
[0] i64 in 17.0 millis
14

oxide.public[1]> (2 * 7) + 12
[1] i64 in 12.9 millis
26
```

Use the range operator (..) to creates slices (array-like structures):

```bash
oxide.public[2]> 1..7
[2] Array in 8.0 millis
[1,2,3,4,5,6]
```

Use the factorial operator (¡):

```bash
oxide.public[3]> 5¡
[3] f64 in 5.3 millis
120.0
```

Use the exponent operators (², ³, .., ⁹):

```bash
oxide.public[4]> 5²
[4] i64 in 5.5 millis
25

oxide.public[5]> 7³
[5] i64 in 6.1 millis
343
```

Use SQL-like updates and queries to create and manage data collections:

```bash
Welcome to Oxide REPL. Enter "q!" to quit.

oxide.public[0]> drop table ns("ldaniels.securities.stocks")
[0] Boolean in 9.6 millis
true

oxide.public[1]> create table ns("ldaniels.securities.stocks") (
    symbol: String(8),
    exchange: String(8),
    last_sale: f64
)
[1] Boolean in 9.5 millis
true

oxide.public[2]> append ns("interpreter.reverse.stocks")
                 from { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 }
[2] i64 in 9.2 millis
1

oxide.public[3]> append ns("interpreter.reverse.stocks")
                 from [
                    { symbol: "TRX", exchange: "OTCBB", last_sale: 0.0076 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                 ]
[3] i64 in 13.8 millis
3

oxide.public[4]> reverse from ns("interpreter.reverse.stocks")
[4] Table ~ 4 row(s) in 10.1 millis
|-------------------------------|
| symbol | exchange | last_sale |
|-------------------------------|
| JET    | NASDAQ   | 32.12     |
| BOOM   | NYSE     | 56.88     |
| TRX    | OTCBB    | 0.0076    |
| ABC    | AMEX     | 12.49     |
|-------------------------------|
```

### API/REST

<a name="create_table"></a>
#### Create a table

The following command will create a new table in the `a.b.stocks` namespace:

```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{
      "columns": [{
          "name": "symbol",
          "param_type": "String(4)",
          "default_value": null
        }, {
          "name": "exchange",
          "param_type": "String(4)",
          "default_value": null
        }, {
          "name": "lastSale",
          "param_type": "f64",
          "default_value": null
        }],
      "indices": [],
      "partitions": []
    }' \
    http://0.0.0.0:8080/a/b/stocks
```

<a name="drop_table"></a>
#### Drop a table

The following command will delete the existing table in the `a.b.stocks` namespace:

```bash
curl -X DELETE http://0.0.0.0:8080/a/b/stocks
```

server response:

```json
1
```

<a name="overwrite_row"></a>
#### Insert/overwrite a row by offset

In this example we insert/overwrite a row into a new or existing table in the `a.b.stocks` namespace:

```bash
curl -X POST \
     -H "Content-Type: application/json" \
     -d '{
      "columns": [{
          "name": "symbol",
          "value": "ABC"
        }, {
          "name": "exchange",
          "value": "NYSE"
        }, {
          "name": "lastSale",
          "value": 56.17
        }],
      "indices": [],
      "partitions": []
    }' \
    http://0.0.0.0:8080/a/b/stocks/100
```

server response:

```json
1
```

<a name="read_row"></a>
#### Retrieve a row by offset

The following command will retrieve the content at offset `100` from the `a.b.stocks` table:

```bash
curl -X GET http://0.0.0.0:8080/a/b/stocks/100
```

server response:

```json
{
  "id": 100,
  "columns": [{
      "name": "symbol",
      "value": "ABC"
    }, {
      "name": "exchange",
      "value": "NYSE"
    }, {
      "name": "lastSale",
      "value": 56.17
    }]
}
```

<a name="delete_row"></a>
#### Delete a row by offset

The following command will delete the existing table in the `a.b.stocks` namespace:

```bash
curl -X DELETE http://0.0.0.0:8080/a/b/stocks/100
```

server response:

```json
1
```
    "#.as_bytes())?;
    Ok(file)
}

fn generate_run_tests(mut file: File) -> std::io::Result<File> {
    file.write(r#"
#### Run the tests

To run the tests (~ 800 tests at the time of writing):

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

pub fn get_examples(model: &Expression) -> Vec<String> {
    match model {
        Expression::ArrayExpression(..) => vec![
            strip_margin(r#"
                |arr := [1, 4, 2, 8, 5, 7]
                |tools::reverse(arr)
            "#, '|')],
        Expression::AsValue(..) => vec![
            "name: 'Tom'".into(),
            "from { name: 'Tom' }".into()
        ],
        Expression::BitwiseAnd(..) => vec!["0b1111 & 0b0101".into()],
        Expression::BitwiseOr(..) => vec!["0b1010 | 0b0101".into()],
        Expression::BitwiseShiftLeft(..) => vec!["20 << 3".into()],
        Expression::BitwiseShiftRight(..) => vec!["20 >> 3".into()],
        Expression::BitwiseXor(..) => vec!["0b1111 ^ 0b0101".into()],
        Expression::CodeBlock(..) => vec![
            strip_margin(r#"
                |result := {
                |    (a, b, sum) := (0, 1, 0)
                |    while sum < 10 {
                |        sum := sum + (a + b)
                |        t := b
                |        b := a + b
                |        a := t
                |    }
                |    sum
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
                |import durations
                |8:::hours()
            "#, '|')],
        Expression::Condition(..) => vec![
            strip_margin(r#"
                    |x := 10
                    |x between 5 and 10
                "#, '|'),
            strip_margin(r#"
                    |x := 10
                    |x betwixt 5 and 10
                "#, '|'),
            strip_margin(r#"
                    |x := 1..8
                    |x contains 7
                "#, '|'),
        ],
        Expression::CurvyArrowLeft(..) => vec![
            strip_margin(r#"
                |stocks := ns("expressions.read_next_row.stocks")
                |table(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: Date)) ~> stocks
                |rows := [{ symbol: "BIZ", exchange: "NYSE" }, { symbol: "GOTO", exchange: "OTC" }]
                |rows ~> stocks
                |// read the last row
                |last_row <~ stocks
                |last_row
            "#, '|')],
        Expression::CurvyArrowRight(..) => vec![
            strip_margin(r#"
                |stocks := ns("expressions.into.stocks")
                |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                |rows := [
                |   { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                |   { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                |   { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                |]
                |rows ~> stocks
            "#, '|')],
        Expression::DatabaseOp(..) => vec![],
        Expression::Directive(..) => vec![],
        Expression::Divide(..) => vec![
            "20.0 / 3".into()
        ],
        Expression::ElementAt(..) => vec![
            strip_margin(r#"
                |arr := [1, 4, 2, 8, 5, 7]
                |arr[3]
            "#, '|')],
        Expression::Feature { .. } => vec![
            strip_margin(r#"
                |import testing
                |Feature "Matches function" {
                |    Scenario "Compare Array contents: Equal" {
                |        assert(matches(
                |            [ 1 "a" "b" "c" ],
                |            [ 1 "a" "b" "c" ]
                |        ))
                |    }
                |    Scenario "Compare Array contents: Not Equal" {
                |        assert(!matches(
                |            [ 1 "a" "b" "c" ],
                |            [ 0 "x" "y" "z" ]
                |        ))
                |    }
                |    Scenario "Compare JSON contents (in sequence)" {
                |        assert(matches(
                |                { first: "Tom" last: "Lane" },
                |                { first: "Tom" last: "Lane" }))
                |    }
                |    Scenario "Compare JSON contents (out of sequence)" {
                |        assert(matches(
                |                { scores: [82 78 99], id: "A1537" },
                |                { id: "A1537", scores: [82 78 99] }))
                |    }
                |}"#, '|')],
        Expression::FnExpression { .. } => vec![
            strip_margin(r#"
                |product := fn (a, b) => a * b
                |product(2, 5)
            "#, '|')],
        Expression::FoldOver(..) => vec![
            "'Hello' |> tools::reverse".to_string()
        ],
        Expression::ForEach(..) => vec![
            strip_margin(r#"
                |foreach row in tools::to_table(['apple', 'berry', 'kiwi', 'lime']) {
                |    oxide::println(row)
                |}
            "#, '|')],
        Expression::From(..) => vec![
            strip_margin(r#"
                |stocks := tools::to_table([
                |   { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                |   { symbol: "GRU", exchange: "NYSE", last_sale: 56.88 },
                |   { symbol: "APK", exchange: "NASDAQ", last_sale: 32.12 }
                |])
                |from stocks where last_sale > 20.0
            "#, '|')],
        Expression::FunctionCall { .. } => vec![],
        Expression::HTTP(..) => vec![
            strip_margin(r#"
                    |stocks := ns("readme.www.stocks")
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
                    |x := 4
                    |if(x > 5) "Yes"
                    |else if(x < 5) "Maybe"
                    |else "No"
                    "#, '|'),
            strip_margin(r#"
                    |fact := fn(n) => iff(n <= 1, 1, n * fact(n - 1))
                    |fact(6)
                    "#, '|'),
        ],
        Expression::Import(..) => vec![
            strip_margin(r#"
                |import tools
                |stocks := to_table([
                |   { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
                |   { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                |   { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                |])
                |stocks
            "#, '|')],
        Expression::Include(..) => vec![],
        Expression::Literal(..) => vec![],
        Expression::Minus(..) => vec![
            "188 - 36".into(),
            strip_margin(r#"
                    |a := (3, 5, 7)
                    |b := (1, 0, 1)
                    |a - b
                "#, '|')
        ],
        Expression::Module(..) => vec![],
        Expression::Modulo(..) => vec![],
        Expression::Multiply(..) => vec![
            strip_margin(r#"
                    |a := (3, 5, 7)
                    |b := (1, 0, 1)
                    |a * b
                "#, '|')
        ],
        Expression::Neg(..) => vec![
            strip_margin(r#"
                |i := 75
                |-i
            "#, '|')],
        Expression::New(..) => vec![
            "new Table(symbol: String(8), exchange: String(8), last_sale: f64)".into()
        ],
        Expression::Ns(..) => vec![],
        Expression::Parameters(..) => vec![],
        Expression::Plus(..) => vec![
            strip_margin(r#"
                    |a := (2, 4, 6)
                    |b := (1, 2, 3)
                    |a + b
                "#, '|'),
        ],
        Expression::PlusPlus(..) => vec![],
        Expression::Pow(..) => vec![],
        Expression::Range(..) => vec![
            strip_margin(r#"
                |range := 1..5
                |tools::reverse(range)
            "#, '|')],
        Expression::Return(..) => vec![],
        Expression::Scenario { .. } => vec![],
        Expression::SetVariable(..) => vec![
            strip_margin(r#"
                    |a := 7
                    |b := 5
                    |a * b
                "#, '|'),
            strip_margin(r#"
                    |(a, b, c) := (3, 5, 7)
                    |a + b + c
                "#, '|')
        ],
        Expression::SetVariables(..) => vec![],
        Expression::StructureExpression(..) => vec![],
        Expression::TupleExpression(..) => vec![],
        Expression::TypeDef(..) => vec![
            "typedef(String(80))".into()
        ],
        Expression::Variable(..) => vec![
            strip_margin(r#"
                |(a, b, c) := (3, 5, 7)
                |c > b
            "#, '|')],
        Expression::Via(..) => vec![
            strip_margin(r#"
                |stocks := ns("readme.via.stocks")
                |drop table stocks
                |table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
                |
                |rows := [
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
            "#, '|')],
        Expression::While { .. } => vec![
            strip_margin(r#"
                while (x < 5) x := x + 1
                x
            "#, '|')],
    }
}

fn get_language_examples() -> Vec<(String, Vec<String>)> {
    use crate::expression::Expression::*;
    let null = Box::new(Literal(TypedValue::Null));
    let models = vec![
        ("Arrays", ArrayExpression(vec![])),
        ("Aliases", AsValue("".into(), null.clone())),
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
        ("Directives", Directive(Directives::MustAck(null.clone()))),
        ("Mathematics: division", Divide(null.clone(), null.clone())),
        ("Arrays: Indexing", ElementAt(null.clone(), null.clone())),
        ("Testing", Feature { title: null.clone(), scenarios: vec![] }),
        ("Functions", FnExpression { params: vec![], body: None, returns: DataType::BooleanType }),
        ("Function-Call", FunctionCall { fx: null.clone(), args: vec![] }),
        ("Iteration", ForEach("".into(), null.clone(), null.clone())),
        ("Query", From(null.clone())),
        ("HTTP", HTTP(HttpMethodCalls::GET(null.clone()))),
        ("if / iff", If { condition: null.clone(), a: null.clone(), b: None }),
        ("Imports", Import(vec![])),
        ("Includes", Include(null.clone())),
        ("Mathematics: subtraction", Minus(null.clone(), null.clone())),
        ("Mathematics: multiplication", Multiply(null.clone(), null.clone())),
        ("Negative", Neg(null.clone())),
        ("Mathematics: addition", Plus(null.clone(), null.clone())),
        ("New Instances", New(null.clone())),
        ("Ranges", Range(null.clone(), null.clone())),
        ("Variable Assignment", SetVariable("".into(), null.clone())),
        ("Structures", StructureExpression(vec![])),
        ("Type Definitions", TypeDef(null.clone())),
        ("Via Clause", Via(null.clone())),
    ];

    let mut examples = models.iter()
        .map(|(title, model)| (title.to_string(), get_examples(&model)))
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
        let mut file = OpenOptions::new()
            .truncate(true).create(true).read(true).write(true)
            .open("../../basics.md")
            .unwrap();
        file = generate_language_examples(file).unwrap();
        file.flush().unwrap();
    }

    #[test]
    fn test_platform_examples() {
        let mut file = OpenOptions::new()
            .truncate(true).create(true).read(true).write(true)
            .open("../../platform.md")
            .unwrap();
        file = generate_platform_examples(file).unwrap();
        file.flush().unwrap();
    }

    #[test]
    fn test_generate_readme() {
        let mut file = OpenOptions::new()
            .truncate(true).create(true).read(true).write(true)
            .open("../../README.md")
            .unwrap();
        file = generate_readme(file).unwrap();
        file.flush().unwrap();
    }
}