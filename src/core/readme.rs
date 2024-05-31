////////////////////////////////////////////////////////////////////
// Oxide README.md Generation
////////////////////////////////////////////////////////////////////

use std::fs::{File, OpenOptions};
use std::io::Write;

fn generate_readme() -> std::io::Result<File> {
    let file = OpenOptions::new()
        .truncate(true).create(true).read(true).write(true)
        .open("../../README.md")?;
    let file = generate_title(file)?;
    let file = generate_project_status(file)?;
    let file = generate_development(file)?;
    let file = generate_run_tests(file)?;
    let file = generate_getting_started(file)?;
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

fn generate_getting_started(mut file: File) -> std::io::Result<File> {
    file.write(r#"
## Getting Started

<a name="REPL"></a>
### REPL

The Oxide REPL is now available, and with it, you can issue commands directly to the server.
Oxide can evaluate basic expressions:

```bash
$ oxide_repl
Welcome to Oxide REPL. Enter "q!" to quit.

oxide.public[1]> 5 + 9
14
oxide.public[2]> (2 * 7) + 12
26
```

Use the range operator (..) to creates slices (array-like structures):

```bash
oxide.public[3]> 1..7
[1,2,3,4,5,6]
```

Use the factorial operator (¡):

```bash
oxide.public[4]> 5¡
120.0
```

Use the exponent operators (², ³)

```bash
oxide.public[4]> 5²
25
oxide.public[5]> 5³
125
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
          "column_type": "String(4)",
          "default_value": null
        }, {
          "name": "exchange",
          "column_type": "String(4)",
          "default_value": null
        }, {
          "name": "lastSale",
          "column_type": "f64",
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

To run the tests (~ 130 tests at the time of writing):

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

fn generate_xxx(mut file: File) -> std::io::Result<File> {
    file.write(r#"
    "#.as_bytes())?;
    Ok(file)
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::readme::generate_readme;

    #[test]
    fn test_generate_readme() {
        generate_readme().unwrap();
    }
}