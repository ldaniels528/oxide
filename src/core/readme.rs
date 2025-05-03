#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Oxide README.md Generation
////////////////////////////////////////////////////////////////////

use crate::interpreter::Interpreter;
use crate::platform::{PlatformOps, PLATFORM_OPCODES};
use crate::row_collection::RowCollection;
use crate::table_renderer::TableRenderer;
use crate::typed_values::TypedValue::TableValue;
use std::fs::{File, OpenOptions};
use std::io::Write;

fn generate_readme() -> std::io::Result<File> {
    let file = OpenOptions::new()
        .truncate(true).create(true).read(true).write(true)
        .open("../../README.md")?;

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

    println!("generate examples...");
    let file = generate_examples(file)?;

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

fn generate_examples(mut file: File) -> std::io::Result<File> {
    writeln!(file, r#"
<a name="examples"></a>
#### Platform Examples
"#)?;

    for op in PLATFORM_OPCODES {
        // header section
        // ex: "oxide::version - ..."
        writeln!(file, "<hr>")?;
        writeln!(file, "<h4>{}::{} &#8212; {}</h4>",
                 op.get_package_name(), op.get_name(), op.get_description())?;

        // write the example body
        let example_body = format!("
<pre>
{}
</pre>",
            op.get_example().trim()
        );
        writeln!(file, "{}", example_body)?;

        // write the results body
        match generate_example_results(op) {
            Ok(out_lines) => file = print_text_block(file, out_lines)?,
            Err(err) => {
                writeln!(file, "ERROR: {}", err.to_string())?;
            }
        }
    }
    Ok(file)
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
    use crate::readme::{generate_examples, generate_readme};
    use std::fs::OpenOptions;

    #[test]
    fn test_generate_examples() {
        let file = OpenOptions::new()
            .truncate(true).create(true).read(true).write(true)
            .open("../../examples.md")
            .unwrap();
        generate_examples(file).unwrap();
    }

    #[test]
    fn test_generate_readme() {
        generate_readme().unwrap();
    }
}