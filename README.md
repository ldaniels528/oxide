
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

## Project Status

- The <a href='#REPL'>REPL</a> is now available, and allows you to issue commands directly to the server.
- The database server is also now available and supports basic CRUD operations via REST for:
  - <a href='#create_table'>creating tables</a>
  - <a href='#drop_table'>dropping tables</a>
  - <a href='#overwrite_row'>insert/overwrite a row by offset</a>
  - <a href='#read_row'>retrieve a row by offset</a>
  - <a href='#delete_row'>delete a row by offset</a>
  - <a href='#rpc'>remote procedure calls</a>

## Development

#### Build the Oxide REPL and Server

```bash
cargo build --release
```

You'll find the executables in `./target/release/`:
* `oxide_repl` is the Oxide REST client / REPL
* `oxide_server` is the Oxide REST Server

#### Run the tests

To run the tests (~ 330 tests at the time of writing):

```bash
cargo test
```
    
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
[2] RowsAffected in 9.2 millis
1

oxide.public[3]> append ns("interpreter.reverse.stocks")
                 from [
                    { symbol: "TRX", exchange: "OTCBB", last_sale: 0.0076 },
                    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
                    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
                 ]
[3] RowsAffected in 13.8 millis
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
    
<a name="examples"></a>
#### Platform Examples

<hr>
<h4>cal::now &#8212; Returns the current local date and time</h4>
<pre>
cal::now()
</pre>
<pre>
2025-03-28T03:15:55.942Z
</pre>
<hr>
<h4>cal::day_of &#8212; Returns the day of the month of a Date</h4>
<pre>

import cal
now():::day_of()

</pre>
<pre>
27
</pre>
<hr>
<h4>cal::hour12 &#8212; Returns the hour of the day of a Date</h4>
<pre>

import cal
now():::hour12()

</pre>
<pre>
8
</pre>
<hr>
<h4>cal::hour24 &#8212; Returns the hour (military time) of the day of a Date</h4>
<pre>

import cal
now():::hour24()

</pre>
<pre>
20
</pre>
<hr>
<h4>cal::minute_of &#8212; Returns the minute of the hour of a Date</h4>
<pre>

import cal
now():::minute_of()

</pre>
<pre>
15
</pre>
<hr>
<h4>cal::month_of &#8212; Returns the month of the year of a Date</h4>
<pre>

import cal
now():::month_of()

</pre>
<pre>
55
</pre>
<hr>
<h4>cal::second_of &#8212; Returns the seconds of the minute of a Date</h4>
<pre>

import cal
now():::second_of()

</pre>
<pre>
55
</pre>
<hr>
<h4>cal::year_of &#8212; Returns the year of a Date</h4>
<pre>

import cal
now():::year_of()

</pre>
<pre>
2025
</pre>
<hr>
<h4>io::create_file &#8212; Creates a new file</h4>
<pre>

io::create_file("quote.json", { symbol: "TRX", exchange: "NYSE", last_sale: 45.32 })

</pre>
<pre>
52
</pre>
<hr>
<h4>io::exists &#8212; Returns true if the source path exists</h4>
<pre>

io::exists("quote.json")

</pre>
<pre>
true
</pre>
<hr>
<h4>io::read_text_file &#8212; Reads the contents of a text file into memory</h4>
<pre>

import io, util
file := "temp_secret.txt"
file:::create_file(md5("**keep**this**secret**"))
file:::read_text_file()

</pre>
<pre>
47338bd5f35bbb239092c36e30775b4a
</pre>
<hr>
<h4>io::stderr &#8212; Writes a string to STDERR</h4>
<pre>
io::stderr("Goodbye Cruel World")
</pre>
<pre>
Ack
</pre>
<hr>
<h4>io::stdout &#8212; Writes a string to STDOUT</h4>
<pre>
io::stdout("Hello World")
</pre>
<pre>
Ack
</pre>
<hr>
<h4>testing::assert &#8212; Evaluates an assertion returning Ack or an error</h4>
<pre>

import testing
assert(matches([ 1 "a" "b" "c" ], [ 1 "a" "b" "c" ]))

</pre>
<pre>
true
</pre>
<hr>
<h4>testing::feature &#8212; Creates a new test feature</h4>
<pre>

import testing
feature("Matches function", {
"Compare Array contents: Equal": fn(ctx) => {
assert(matches(
[ 1 "a" "b" "c" ],
[ 1 "a" "b" "c" ]))
},
"Compare Array contents: Not Equal": fn(ctx) => {
assert(!matches(
[ 1 "a" "b" "c" ],
[ 0 "x" "y" "z" ]))
},
"Compare JSON contents (in sequence)": fn(ctx) => {
assert(matches(
{ first: "Tom" last: "Lane" },
{ first: "Tom" last: "Lane" }))
},
"Compare JSON contents (out of sequence)": fn(ctx) => {
assert(matches(
{ scores: [82 78 99], id: "A1537" },
{ id: "A1537", scores: [82 78 99] }))
}
})
</pre>
<pre>
|--------------------------------------------------------------------------------------------------------------------------|
| id | level | item                                                                                      | passed | result |
|--------------------------------------------------------------------------------------------------------------------------|
| 0  | 0     | Matches function                                                                          | true   | Ack    |
| 1  | 1     | Compare Array contents: Equal                                                             | true   | Ack    |
| 2  | 2     | assert(matches([1, "a", "b", "c"], [1, "a", "b", "c"]))                                   | true   | true   |
| 3  | 1     | Compare Array contents: Not Equal                                                         | true   | Ack    |
| 4  | 2     | assert(!matches([1, "a", "b", "c"], [0, "x", "y", "z"]))                                  | true   | true   |
| 5  | 1     | Compare JSON contents (in sequence)                                                       | true   | Ack    |
| 6  | 2     | assert(matches({first: "Tom", last: "Lane"}, {first: "Tom", last: "Lane"}))               | true   | true   |
| 7  | 1     | Compare JSON contents (out of sequence)                                                   | true   | Ack    |
| 8  | 2     | assert(matches({scores: [82, 78, 99], id: "A1537"}, {id: "A1537", scores: [82, 78, 99]})) | true   | true   |
|--------------------------------------------------------------------------------------------------------------------------|
</pre>
<hr>
<h4>testing::matches &#8212; Compares two values</h4>
<pre>

import testing::matches
a := { scores: [82, 78, 99], first: "Tom", last: "Lane" }
b := { last: "Lane", first: "Tom", scores: [82, 78, 99] }
matches(a, b)

</pre>
<pre>
true
</pre>
<hr>
<h4>testing::type_of &#8212; Returns the type of a value</h4>
<pre>
testing::type_of([12, 76, 444])
</pre>
<pre>
Array(3)
</pre>
<hr>
<h4>os::call &#8212; Invokes an operating system application</h4>
<pre>

create table ns("platform.os.call") (
symbol: String(8),
exchange: String(8),
last_sale: f64
)
os::call("chmod", "777", oxide::home())

</pre>
<pre>

</pre>
<hr>
<h4>os::clear &#8212; Clears the terminal/screen</h4>
<pre>
os::clear()
</pre>
<pre>
Ack
</pre>
<hr>
<h4>os::current_dir &#8212; Returns the current directory</h4>
<pre>

import str
cur_dir := os::current_dir()
prefix := iff(cur_dir:::ends_with("core"), "../..", ".")
path_str := prefix + "/demoes/language/include_file.oxide"
include path_str

</pre>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 0  | ABC    | AMEX     | 12.49     |
| 1  | BOOM   | NYSE     | 56.88     |
| 2  | JET    | NASDAQ   | 32.12     |
|------------------------------------|
</pre>
<hr>
<h4>os::env &#8212; Returns a table of the OS environment variables</h4>
<pre>
os::env()
</pre>
<pre>
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id | key                        | value                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 0  | BUN_INSTALL                | /Users/ldaniels/.bun                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 1  | CARGO                      | /Users/ldaniels/.rustup/toolchains/stable-aarch64-apple-darwin/bin/cargo                                                                                                                                                                                                                                                                                                                                                               |
| 2  | CARGO_HOME                 | /Users/ldaniels/.cargo                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 3  | CARGO_MANIFEST_DIR         | /Users/ldaniels/GitHub/oxide/src/core                                                                                                                                                                                                                                                                                                                                                                                                  |
| 4  | CARGO_MANIFEST_PATH        | /Users/ldaniels/GitHub/oxide/src/core/Cargo.toml                                                                                                                                                                                                                                                                                                                                                                                       |
| 5  | CARGO_PKG_AUTHORS          |                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 6  | CARGO_PKG_DESCRIPTION      |                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 7  | CARGO_PKG_HOMEPAGE         |                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 8  | CARGO_PKG_LICENSE          |                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 9  | CARGO_PKG_LICENSE_FILE     |                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 10 | CARGO_PKG_NAME             | core                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 11 | CARGO_PKG_README           |                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 12 | CARGO_PKG_REPOSITORY       |                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 13 | CARGO_PKG_RUST_VERSION     |                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 14 | CARGO_PKG_VERSION          | 0.1.0                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 15 | CARGO_PKG_VERSION_MAJOR    | 0                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 16 | CARGO_PKG_VERSION_MINOR    | 1                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 17 | CARGO_PKG_VERSION_PATCH    | 0                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 18 | CARGO_PKG_VERSION_PRE      |                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 19 | COMMAND_MODE               | unix2003                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 20 | DYLD_FALLBACK_LIBRARY_PATH | /Users/ldaniels/GitHub/oxide/target/debug/build/zstd-sys-3c7e39002162ae64/out:/Users/ldaniels/GitHub/oxide/target/debug/deps:/Users/ldaniels/GitHub/oxide/target/debug:/Users/ldaniels/.rustup/toolchains/stable-aarch64-apple-darwin/lib/rustlib/aarch64-apple-darwin/lib:/Users/ldaniels/.rustup/toolchains/stable-aarch64-apple-darwin/lib:/Users/ldaniels/lib:/usr/local/lib:/usr/lib                                              |
| 21 | HOME                       | /Users/ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 22 | IDEA_INITIAL_DIRECTORY     | /                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 23 | JAVA_HOME                  | /Users/ldaniels/.sdkman/candidates/java/current                                                                                                                                                                                                                                                                                                                                                                                        |
| 24 | LC_CTYPE                   | en_US.UTF-8                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 25 | LOGNAME                    | ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 26 | OLDPWD                     | /                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 27 | PATH                       | /Users/ldaniels/.bun/bin:/Users/ldaniels/.sdkman/candidates/java/current/bin:/usr/local/bin:/System/Cryptexes/App/usr/bin:/usr/bin:/bin:/usr/sbin:/sbin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/local/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/appleinternal/bin:/Users/ldaniels/.cargo/bin:/opt/homebrew/bin:. |
| 28 | PWD                        | /Users/ldaniels/GitHub/oxide                                                                                                                                                                                                                                                                                                                                                                                                           |
| 29 | RUSTC                      | /Users/ldaniels/.cargo/bin/rustc                                                                                                                                                                                                                                                                                                                                                                                                       |
| 30 | RUSTC_BOOTSTRAP            | 1                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 31 | RUSTUP_HOME                | /Users/ldaniels/.rustup                                                                                                                                                                                                                                                                                                                                                                                                                |
| 32 | RUSTUP_TOOLCHAIN           | stable-aarch64-apple-darwin                                                                                                                                                                                                                                                                                                                                                                                                            |
| 33 | RUST_BACKTRACE             | short                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 34 | RUST_RECURSION_COUNT       | 1                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 35 | SDKMAN_CANDIDATES_API      | https://api.sdkman.io/2                                                                                                                                                                                                                                                                                                                                                                                                                |
| 36 | SDKMAN_CANDIDATES_DIR      | /Users/ldaniels/.sdkman/candidates                                                                                                                                                                                                                                                                                                                                                                                                     |
| 37 | SDKMAN_DIR                 | /Users/ldaniels/.sdkman                                                                                                                                                                                                                                                                                                                                                                                                                |
| 38 | SDKMAN_PLATFORM            | darwinarm64                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 39 | SHELL                      | /bin/zsh                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 40 | SSH_AUTH_SOCK              | /private/tmp/com.apple.launchd.s3Detqy0KH/Listeners                                                                                                                                                                                                                                                                                                                                                                                    |
| 41 | TERM                       | ansi                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 42 | TMPDIR                     | /var/folders/ld/hwrvzn011w79gftyb6vj8mg40000gn/T/                                                                                                                                                                                                                                                                                                                                                                                      |
| 43 | USER                       | ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 44 | XPC_FLAGS                  | 0x0                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 45 | XPC_SERVICE_NAME           | application.com.jetbrains.intellij.505803.58851138                                                                                                                                                                                                                                                                                                                                                                                     |
| 46 | __CFBundleIdentifier       | com.jetbrains.intellij                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 47 | __CF_USER_TEXT_ENCODING    | 0x1F5:0x0:0x0                                                                                                                                                                                                                                                                                                                                                                                                                          |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
</pre>
<hr>
<h4>oxide::compile &#8212; Compiles source code from a string input</h4>
<pre>

code := oxide::compile("2 ** 4")
code()

</pre>
<pre>
16
</pre>
<hr>
<h4>oxide::debug &#8212; Compiles source code from a string input; returning a debug string</h4>
<pre>
oxide::debug("2 ** 4")
</pre>
<pre>
Pow(Literal(Number(I64Value(2))), Literal(Number(I64Value(4))))
</pre>
<hr>
<h4>oxide::eval &#8212; Evaluates a string containing Oxide code</h4>
<pre>

a := 'Hello '
b := 'World'
oxide::eval("a + b")

</pre>
<pre>
Hello World
</pre>
<hr>
<h4>oxide::help &#8212; Integrated help function</h4>
<pre>
from oxide::help() limit 3
</pre>
<pre>
|-------------------------------------------------------------------------------------------------------------------------------------|
| id | name        | module | signature                                 | description                                       | returns |
|-------------------------------------------------------------------------------------------------------------------------------------|
| 0  | to_string   | str    | str::to_string(x)                         | Converts a value to its text-based representation | String  |
| 1  | substring   | str    | str::substring(s: String, m: i64, n: i64) | Returns a substring of string `s` from `m` to `n` | String  |
| 2  | starts_with | str    | str::starts_with(a: String, b: String)    | Returns true if string `a` starts with string `b` | Boolean |
|-------------------------------------------------------------------------------------------------------------------------------------|
</pre>
<hr>
<h4>oxide::history &#8212; Returns all commands successfully executed during the session</h4>
<pre>
from oxide::history() limit 3
</pre>
<pre>
|-------------------------------------------------------------------|
| id | session_id    | user_id | cpu_time_ms | input                |
|-------------------------------------------------------------------|
| 0  | 1736823002231 | 501     | 2.081       | import oxide; help() |
| 1  | 1736823002627 | 501     | 3.596       | import oxide; help() |
| 2  | 1736827273725 | 501     | 0.971       | import oxide; help() |
|-------------------------------------------------------------------|
</pre>
<hr>
<h4>oxide::home &#8212; Returns the Oxide home directory path</h4>
<pre>
oxide::home()
</pre>
<pre>
./oxide_db
</pre>
<hr>
<h4>oxide::println &#8212; Print line function</h4>
<pre>
oxide::println("Hello World")
</pre>
<pre>
Ack
</pre>
<hr>
<h4>oxide::reset &#8212; Clears the scope of all user-defined objects</h4>
<pre>
oxide::reset()
</pre>
<pre>
Ack
</pre>
<hr>
<h4>oxide::uuid &#8212; Returns a random 128-bit UUID</h4>
<pre>
oxide::uuid()
</pre>
<pre>
0a5749e5-1795-484b-9e6d-a8c5e66b85b6
</pre>
<hr>
<h4>oxide::version &#8212; Returns the Oxide version</h4>
<pre>
oxide::version()
</pre>
<pre>
0.3
</pre>
<hr>
<h4>str::ends_with &#8212; Returns true if string `a` ends with string `b`</h4>
<pre>
str::ends_with('Hello World', 'World')
</pre>
<pre>
true
</pre>
<hr>
<h4>str::format &#8212; Returns an argument-formatted string</h4>
<pre>
str::format("This {} the {}", "is", "way")
</pre>
<pre>
This is the way
</pre>
<hr>
<h4>str::index_of &#8212; Returns the index of string `b` in string `a`</h4>
<pre>
str::index_of('The little brown fox', 'brown')
</pre>
<pre>
11
</pre>
<hr>
<h4>str::join &#8212; Combines an array into a string</h4>
<pre>
str::join(['1', 5, 9, '13'], ', ')
</pre>
<pre>
1, 5, 9, 13
</pre>
<hr>
<h4>str::left &#8212; Returns n-characters from left-to-right</h4>
<pre>
str::left('Hello World', 5)
</pre>
<pre>
Hello
</pre>
<hr>
<h4>str::len &#8212; Returns the number of characters contained in the string</h4>
<pre>
str::len('The little brown fox')
</pre>
<pre>
20
</pre>
<hr>
<h4>str::right &#8212; Returns n-characters from right-to-left</h4>
<pre>
str::right('Hello World', 5)
</pre>
<pre>
World
</pre>
<hr>
<h4>str::split &#8212; Splits string `a` by delimiter string `b`</h4>
<pre>
str::split('Hello,there World', ' ,')
</pre>
<pre>
|------------|
| id | value |
|------------|
| 0  | Hello |
| 1  | there |
| 2  | World |
|------------|
</pre>
<hr>
<h4>str::starts_with &#8212; Returns true if string `a` starts with string `b`</h4>
<pre>
str::starts_with('Hello World', 'World')
</pre>
<pre>
false
</pre>
<hr>
<h4>str::substring &#8212; Returns a substring of string `s` from `m` to `n`</h4>
<pre>
str::substring('Hello World', 0, 5)
</pre>
<pre>
Hello
</pre>
<hr>
<h4>str::to_string &#8212; Converts a value to its text-based representation</h4>
<pre>
str::to_string(125.75)
</pre>
<pre>
125.75
</pre>
<hr>
<h4>tools::compact &#8212; Shrinks a table by removing deleted rows</h4>
<pre>

[+] stocks := ns("platform.compact.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "DMX", exchange: "NYSE", last_sale: 99.99 },
{ symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
{ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
{ symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
{ symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 },
{ symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
[+] delete from stocks where last_sale > 1.0
[+] from stocks

</pre>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 1  | UNO    | OTC      | 0.2456    |
| 3  | GOTO   | OTC      | 0.1428    |
| 5  | BOOM   | NASDAQ   | 0.0872    |
|------------------------------------|
</pre>
<hr>
<h4>tools::describe &#8212; Describes a table or structure</h4>
<pre>

tools::describe({ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 })

</pre>
<pre>
|----------------------------------------------------------|
| id | name      | type      | default_value | is_nullable |
|----------------------------------------------------------|
| 0  | symbol    | String(3) | BIZ           | true        |
| 1  | exchange  | String(4) | NYSE          | true        |
| 2  | last_sale | f64       | 23.66         | true        |
|----------------------------------------------------------|
</pre>
<hr>
<h4>tools::fetch &#8212; Retrieves a raw structure from a table</h4>
<pre>

[+] stocks := ns("platform.fetch.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
{ symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
[+] tools::fetch(stocks, 2)

</pre>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 2  | JET    | NASDAQ   | 32.12     |
|------------------------------------|
</pre>
<hr>
<h4>tools::pop &#8212; Removes and returns a value or object from a Sequence</h4>
<pre>

import tools
[+] stocks := ns("platform.pop.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
{ symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
[+] stocks::pop(stocks)

</pre>
<pre>
Illegal structure platform.pop.stocks
</pre>
<hr>
<h4>tools::push &#8212; Appends a value or object to a Sequence</h4>
<pre>

import tools
[+] stocks := ns("platform.push.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
{ symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
[+] stocks::push({ symbol: "XYZ", exchange: "NASDAQ", last_sale: 24.78 })
[+] stocks

</pre>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 0  | ABC    | AMEX     | 12.49     |
| 1  | BOOM   | NYSE     | 56.88     |
| 2  | JET    | NASDAQ   | 32.12     |
|------------------------------------|
</pre>
<hr>
<h4>tools::reverse &#8212; Returns a reverse copy of a table, string or array</h4>
<pre>

import tools
to_table(reverse(['cat', 'dog', 'ferret', 'mouse']))

</pre>
<pre>
|-------------|
| id | value  |
|-------------|
| 0  | mouse  |
| 1  | ferret |
| 2  | dog    |
| 3  | cat    |
|-------------|
</pre>
<hr>
<h4>tools::scan &#8212; Returns existence metadata for a table</h4>
<pre>

[+] import tools
[+] stocks := ns("platform.scan.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
{ symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
{ symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
{ symbol: "GOTO", exchange: "OTC", last_sale: 0.1442 },
{ symbol: "XYZ", exchange: "NYSE", last_sale: 0.0289 }] ~> stocks
[+] delete from stocks where last_sale > 1.0
[+] stocks:::scan()

</pre>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 0  | ABC    | AMEX     | 12.33     |
| 1  | UNO    | OTC      | 0.2456    |
| 2  | BIZ    | NYSE     | 9.775     |
| 3  | GOTO   | OTC      | 0.1442    |
| 4  | XYZ    | NYSE     | 0.0289    |
|------------------------------------|
</pre>
<hr>
<h4>tools::to_array &#8212; Converts a collection into an array</h4>
<pre>
tools::to_array("Hello")
</pre>
<pre>
|------------|
| id | value |
|------------|
| 0  | H     |
| 1  | e     |
| 2  | l     |
| 3  | l     |
| 4  | o     |
|------------|
</pre>
<hr>
<h4>tools::to_csv &#8212; Converts a collection to CSV format</h4>
<pre>

import tools::to_csv
[+] stocks := ns("platform.csv.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
{ symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
{ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
{ symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
{ symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
stocks:::to_csv()

</pre>
<pre>
|-----------------------------|
| id | value                  |
|-----------------------------|
| 0  | "ABC","AMEX",11.11     |
| 1  | "UNO","OTC",0.2456     |
| 2  | "BIZ","NYSE",23.66     |
| 3  | "GOTO","OTC",0.1428    |
| 4  | "BOOM","NASDAQ",0.0872 |
|-----------------------------|
</pre>
<hr>
<h4>tools::to_json &#8212; Converts a collection to JSON format</h4>
<pre>

import tools::to_json
[+] stocks := ns("platform.json.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
{ symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
{ symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
{ symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
{ symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
stocks:::to_json()

</pre>
<pre>
|---------------------------------------------------------------|
| id | value                                                    |
|---------------------------------------------------------------|
| 0  | {"symbol":"ABC","exchange":"AMEX","last_sale":11.11}     |
| 1  | {"symbol":"UNO","exchange":"OTC","last_sale":0.2456}     |
| 2  | {"symbol":"BIZ","exchange":"NYSE","last_sale":23.66}     |
| 3  | {"symbol":"GOTO","exchange":"OTC","last_sale":0.1428}    |
| 4  | {"symbol":"BOOM","exchange":"NASDAQ","last_sale":0.0872} |
|---------------------------------------------------------------|
</pre>
<hr>
<h4>tools::to_table &#8212; Converts an object into a to_table</h4>
<pre>

tools::to_table(['cat', 'dog', 'ferret', 'mouse'])

</pre>
<pre>
|-------------|
| id | value  |
|-------------|
| 0  | cat    |
| 1  | dog    |
| 2  | ferret |
| 3  | mouse  |
|-------------|
</pre>
<hr>
<h4>util::base64 &#8212; Translates bytes into Base 64</h4>
<pre>
util::base64('Hello World')
</pre>
<pre>
SGVsbG8gV29ybGQ=
</pre>
<hr>
<h4>util::to_binary &#8212; Translates a numeric value into binary</h4>
<pre>
util::to_binary(0b1011 & 0b1101)
</pre>
<pre>
1001
</pre>
<hr>
<h4>util::hex &#8212; Translates bytes into hexadecimal</h4>
<pre>
util::hex('Hello World')
</pre>
<pre>
48656c6c6f20576f726c64
</pre>
<hr>
<h4>util::md5 &#8212; Creates a MD5 digest</h4>
<pre>
util::md5('Hello World')
</pre>
<pre>
b10a8db164e0754105b7a99be72e3fe5
</pre>
<hr>
<h4>util::to_ascii &#8212; Converts an integer to ASCII</h4>
<pre>
util::to_ascii(177)
</pre>
<pre>
±
</pre>
<hr>
<h4>util::to_date &#8212; Converts a value to Date</h4>
<pre>
util::to_date(177)
</pre>
<pre>
1970-01-01T00:00:00.177Z
</pre>
<hr>
<h4>util::to_f32 &#8212; Converts a value to f32</h4>
<pre>
util::to_f32(4321)
</pre>
<pre>
4321
</pre>
<hr>
<h4>util::to_f64 &#8212; Converts a value to f64</h4>
<pre>
util::to_f64(4321)
</pre>
<pre>
4321
</pre>
<hr>
<h4>util::to_i8 &#8212; Converts a value to i8</h4>
<pre>
util::to_i8(88)
</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_i16 &#8212; Converts a value to i16</h4>
<pre>
util::to_i16(88)
</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_i32 &#8212; Converts a value to i32</h4>
<pre>
util::to_i32(88)
</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_i64 &#8212; Converts a value to i64</h4>
<pre>
util::to_i64(88)
</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_i128 &#8212; Converts a value to i128</h4>
<pre>
util::to_i128(88)
</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_u8 &#8212; Converts a value to u8</h4>
<pre>
util::to_u8(88)
</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_u16 &#8212; Converts a value to u16</h4>
<pre>
util::to_u16(88)
</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_u32 &#8212; Converts a value to u32</h4>
<pre>
util::to_u32(88)
</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_u64 &#8212; Converts a value to u64</h4>
<pre>
util::to_u64(88)
</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_u128 &#8212; Converts a value to u128</h4>
<pre>
util::to_u128(88)
</pre>
<pre>
00000000-0000-0000-0000-000000000058
</pre>
<hr>
<h4>www::url_decode &#8212; Decodes a URL-encoded string</h4>
<pre>
www::url_decode('http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998')
</pre>
<pre>
http://shocktrade.com?name=the hero&t=9998
</pre>
<hr>
<h4>www::url_encode &#8212; Encodes a URL string</h4>
<pre>
www::url_encode('http://shocktrade.com?name=the hero&t=9998')
</pre>
<pre>
http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998
</pre>
<hr>
<h4>www::serve &#8212; Starts a local HTTP service</h4>
<pre>

[+] www::serve(8822)
[+] stocks := ns("platform.www.quotes")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 },
{ symbol: "BOX", exchange: "NYSE", last_sale: 56.88 },
{ symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
{ symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }] ~> stocks
GET http://localhost:8822/platform/www/quotes/1/4

</pre>
<pre>
|------------------------------------|
| id | exchange | last_sale | symbol |
|------------------------------------|
| 0  | NYSE     | 56.88     | BOX    |
| 1  | NASDAQ   | 32.12     | JET    |
| 2  | AMEX     | 12.49     | ABC    |
|------------------------------------|
</pre>

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
    