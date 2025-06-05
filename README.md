
🧪 Oxide — A Lightweight, Modern Language for Data, APIs & Automation
========================================================================

**Oxide** is a clean, expressive scripting language built for the modern developer. Whether you're transforming data, automating workflows, building APIs, or exploring time-based events, Oxide empowers you with elegant syntax and a practical standard library—designed to make complex operations feel intuitive.

---

## Table of Contents
* <a href="#why_choose_it">Why Choose Oxide?</a>
* <a href="#what_can_you_do">What Can You Do with Oxide?</a>
* <a href="#who_is_it_for">Who Is Oxide For?</a>
* <a href="#getting_started">Getting Started</a>
* <a href="#operators">Operators</a>
* <a href="#core_examples">Core/Language examples</a>
* <a href="#platform_examples">Platform examples</a>

---

<a name="why_choose_it"></a>
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

<a name="what_can_you_do"></a>
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
arr:::filter(x -> (x % 2) == 0):::map(x -> x * 10)
```

---

<a name="who_is_it_for"></a>
## 👥 Who Is Oxide For?

- **Data Engineers & Analysts** — quick scripting for time and table-based operations.
- **Web Developers** — seamless API interactions and response transformations.
- **Scripters & Hackers** — ideal for automation, file operations, and glue code.
- **Language Enthusiasts** — a functional-style pipeline DSL with just enough structure.

---

<a name="getting_started"></a>
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

## 📦 Core language & Platform

The remainder of this document showcases categorized usage examples across Oxide's standard modules including:

- `arrays`, `cal`, `durations`, `io`, `math`, `os`, `oxide`, `str`, `tools`, `util`, and `www`.

To improve navigation, consider splitting the examples into separate markdown files or auto-generating docs from code annotations using a tool like `mdBook`, `Docusaurus`, or a custom Rust doc generator.

<a name="operators"></a>
### 🧮 Binary Operators Reference
    

Oxide provides a rich set of binary operators for arithmetic, logic, assignment, comparison, bitwise manipulation, and expressive data flow. This document summarizes the available operators and their intended semantics.

---

## 🔢 Arithmetic Operators

| Operator    | Meaning           |
|-------------|-------------------|
| `+`         | Addition          |
| `++`        | Concatenation or Join |
| `-`         | Subtraction       |
| `*`, `×`    | Multiplication    |
| `/`, `÷`    | Division          |
| `%`         | Modulo            |
| `**`        | Power (Exponentiation) |

---

## 🧠 Assignment Operators

| Operator    | Meaning                      |
|-------------|------------------------------|
| `=`         | Assign variable              |
| `+=`        | Add and assign               |
| `-=`        | Subtract and assign          |
| `*=`        | Multiply and assign          |
| `/=`        | Divide and assign            |
| `%=`        | Modulo and assign            |
| `&=`        | Bitwise AND and assign       |
| `⎜=`        | Bitwise OR and assign        |
| `^=`        | Bitwise XOR and assign       |
| `?=`        | Coalesce and assign          |
| `&&=`       | Logical AND and assign       |
| `⎜⎜=`       | Logical OR and assign        |
| `:=`        | Declare and assign expression |

---

## 🧮 Bitwise Operators

| Operator    | Meaning              |
|------------|----------------------|
| `&`         | Bitwise AND          |
| `⎜`         | Bitwise OR           |
| `^`         | Bitwise XOR          |
| `<<`        | Shift Left           |
| `>>`        | Shift Right          |

---

## 🔍 Comparison and Logical Operators

| Operator      | Meaning                     |
|---------------|-----------------------------|
| `==`, `is`     | Equal                       |
| `!=`, `isnt`   | Not Equal                   |
| `>`            | Greater Than                |
| `>=`           | Greater Than or Equal       |
| `<`            | Less Than                   |
| `<=`           | Less Than or Equal          |
| `in`           | Value is in Range or Set    |
| `like`         | SQL-style pattern match     |
| `matches`      | Regular Expression match    |
| `&&`           | Logical AND                 |
| `⎜⎜`           | Logical OR                  |
| `?`            | Null⎜Undefined Coalescing   |

---

## 🧪 Special Operators

| Operator     | Meaning / Use Case               |
|--------------|----------------------------------|
| `:`           | Alias (value name alias)        |
| `::`          | Namespacing or qualified access |
| `:::`         | Extended namespacing or chaining |
| `<~`          | Curvy arrow (left)              |
| `~>`          | Curvy arrow (right)             |
| `->`          | Function application            |
| `..`          | Exclusive Range (`a..b`)        |
| `..=`         | Inclusive Range (`a..=b`)       |

---

## 🔀 Data Flow & Piping

| Operator     | Meaning / Use Case                 |
|--------------|------------------------------------|
| `⎜>`          | Pipe Forward (`val |> fn`)        |
| `⎜>>`         | Double Pipe Forward (custom logic)|

---

<a name="core_examples"></a>
### 📖 Core Language Examples
    
<hr>
<h4>▶️ Aliases</h4>
<h5>example¹</h5>
<pre>name: 'Tom'</pre>
<h5>results</h5>
<pre>
"Tom"
</pre>
<h5>example²</h5>
<pre>from { name: 'Tom' }</pre>
<h5>results</h5>
<pre>
|-----------|
| id | name |
|-----------|
| 0  | Tom  |
|-----------|
</pre>
<hr>
<h4>▶️ Arrays</h4>
<h5>example¹</h5>
<pre>// Arrays can be defined via ranges

1..7</pre>
<h5>results</h5>
<pre>
[1, 2, 3, 4, 5, 6]
</pre>
<h5>example²</h5>
<pre>// Arrays can be created using literals

[1, 4, 2, 8, 5, 7]</pre>
<h5>results</h5>
<pre>
[1, 4, 2, 8, 5, 7]
</pre>
<h5>example³</h5>
<pre>// Arrays may be destructured to assign multiple variables

let [a, b, c] = [3, 5, 7]
a + b + c</pre>
<h5>results</h5>
<pre>
15
</pre>
<h5>example⁴</h5>
<pre>// Arrays can be transformed via the 'arrays' package

arrays::reverse([1, 4, 2, 8, 5, 7])</pre>
<h5>results</h5>
<pre>
[7, 5, 8, 2, 4, 1]
</pre>
<hr>
<h4>▶️ Arrays: Indexing</h4>
<h5>example¹</h5>
<pre>let arr = [1, 4, 2, 8, 5, 7]
arr[3]</pre>
<h5>results</h5>
<pre>
8
</pre>
<hr>
<h4>▶️ Assignment (expression)</h4>
<h5>example¹</h5>
<pre>// Use ":=" to simultaneously assign a value and return the assigned value

let i = 0
while (i < 5) yield (i := i + 1) * 3</pre>
<h5>results</h5>
<pre>
[3, 6, 9, 12, 15]
</pre>
<hr>
<h4>▶️ Assignment (statement)</h4>
<h5>example¹</h5>
<pre>let a = 3
let b = 5
let c = 7
a + b + c</pre>
<h5>results</h5>
<pre>
15
</pre>
<hr>
<h4>▶️ Bitwise And</h4>
<h5>example¹</h5>
<pre>0b1111 & 0b0101</pre>
<h5>results</h5>
<pre>
5
</pre>
<hr>
<h4>▶️ Bitwise Or</h4>
<h5>example¹</h5>
<pre>0b1010 | 0b0101</pre>
<h5>results</h5>
<pre>
15
</pre>
<hr>
<h4>▶️ Bitwise Shift-Left</h4>
<h5>example¹</h5>
<pre>20 << 3</pre>
<h5>results</h5>
<pre>
160
</pre>
<hr>
<h4>▶️ Bitwise Shift-Right</h4>
<h5>example¹</h5>
<pre>20 >> 3</pre>
<h5>results</h5>
<pre>
2
</pre>
<hr>
<h4>▶️ Bitwise XOR</h4>
<h5>example¹</h5>
<pre>0b1111 ^ 0b0101</pre>
<h5>results</h5>
<pre>
10
</pre>
<hr>
<h4>▶️ Coalesce</h4>
<h5>example¹</h5>
<pre>"Hello" ? "it was null or undefined"</pre>
<h5>results</h5>
<pre>
"Hello"
</pre>
<h5>example²</h5>
<pre>null ? "it was null or undefined"</pre>
<h5>results</h5>
<pre>
"it was null or undefined"
</pre>
<h5>example³</h5>
<pre>undefined ? "it was null or undefined"</pre>
<h5>results</h5>
<pre>
"it was null or undefined"
</pre>
<hr>
<h4>▶️ Coalesce Error</h4>
<h5>example¹</h5>
<pre>"No problem" !? "An error occurred"</pre>
<h5>results</h5>
<pre>
"No problem"
</pre>
<h5>example²</h5>
<pre>(throw "Boom!") !? "An error occurred"</pre>
<h5>results</h5>
<pre>
"An error occurred"
</pre>
<hr>
<h4>▶️ Code Block</h4>
<h5>example¹</h5>
<pre>result = {
    let (a, b) = (5, 9)
    a + b
}
result</pre>
<h5>results</h5>
<pre>
14
</pre>
<hr>
<h4>▶️ Conditionals</h4>
<h5>example¹</h5>
<pre>let x = 10
x in 5..=10</pre>
<h5>results</h5>
<pre>
true
</pre>
<h5>example²</h5>
<pre>let x = 10
x in 5..10</pre>
<h5>results</h5>
<pre>
false
</pre>
<h5>example³</h5>
<pre>let x = 1..8
x contains 7</pre>
<h5>results</h5>
<pre>
true
</pre>
<hr>
<h4>▶️ Curvy-Arrow Left</h4>
<h5>example¹</h5>
<pre>stocks = nsd::save(
   "expressions.read_next_row.stocks",
   Table::new(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: Date))
)
rows = [{ symbol: "BIZ", exchange: "NYSE" }, { symbol: "GOTO", exchange: "OTC" }]
rows ~> stocks
// read the last row
last_row <~ stocks
last_row</pre>
<h5>results</h5>
<pre>
{"exchange":"OTC","history":[],"symbol":"GOTO"}
</pre>
<hr>
<h4>▶️ Curvy-Arrow Right</h4>
<h5>example¹</h5>
<pre>stocks = nsd::save(
   "expressions.into.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
rows = [
   { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
   { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
   { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
]
rows ~> stocks</pre>
<h5>results</h5>
<pre>
3
</pre>
<hr>
<h4>▶️ Do-While expression</h4>
<h5>example¹</h5>
<pre>let i = 0
do {
    i = i + 1
    yield i * 2
} while (i < 5)</pre>
<h5>results</h5>
<pre>
[2, 4, 6, 8, 10]
</pre>
<hr>
<h4>▶️ Function Pipelines (destructuring)</h4>
<h5>example¹</h5>
<pre>use tools::reverse
result = 'Hello' |> reverse
result</pre>
<h5>results</h5>
<pre>
"olleH"
</pre>
<h5>example²</h5>
<pre>// arrays, tuples and structures can be deconstructed into arguments

fn add(a, b) -> a + b
fn inverse(a) -> 1.0 / a
result = (2, 3) |>> add |> inverse
result</pre>
<h5>results</h5>
<pre>
0.2
</pre>
<hr>
<h4>▶️ HTTP</h4>
<h5>example¹</h5>
<pre>stocks = nsd::save(
   "readme.www.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
www::serve(8855)</pre>
<h5>results</h5>
<pre>
true
</pre>
<h5>example²</h5>
<pre>POST {
    url: http://localhost:8855/platform/www/stocks/0
    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }
}</pre>
<h5>results</h5>
<pre>
3
</pre>
<h5>example³</h5>
<pre>GET http://localhost:8855/platform/www/stocks/0</pre>
<h5>results</h5>
<pre>
{}
</pre>
<h5>example⁴</h5>
<pre>HEAD http://localhost:8855/platform/www/stocks/0</pre>
<h5>results</h5>
<pre>
{content-length: "81", content-type: "application/json", date: "Thu, 05 Jun 2025 22:06:01 GMT"}
</pre>
<h5>example⁵</h5>
<pre>PUT {
    url: http://localhost:8855/platform/www/stocks/0
    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.79 }
}</pre>
<h5>results</h5>
<pre>
1
</pre>
<h5>example⁶</h5>
<pre>GET http://localhost:8855/platform/www/stocks/0</pre>
<h5>results</h5>
<pre>
{exchange: "AMEX", last_sale: 11.79, symbol: "ABC"}
</pre>
<h5>example⁷</h5>
<pre>PATCH {
    url: http://localhost:8855/platform/www/stocks/0
    body: { last_sale: 11.81 }
}</pre>
<h5>results</h5>
<pre>
1
</pre>
<h5>example⁸</h5>
<pre>GET http://localhost:8855/platform/www/stocks/0</pre>
<h5>results</h5>
<pre>
{exchange: "AMEX", last_sale: 11.81, symbol: "ABC"}
</pre>
<h5>example⁹</h5>
<pre>DELETE http://localhost:8855/platform/www/stocks/0</pre>
<h5>results</h5>
<pre>
1
</pre>
<h5>example¹⁰</h5>
<pre>GET http://localhost:8855/platform/www/stocks/0</pre>
<h5>results</h5>
<pre>
{}
</pre>
<hr>
<h4>▶️ IF expression</h4>
<h5>example¹</h5>
<pre>// Oxide provides an if-else statement

let x = 4
if(x > 5) "Yes"
else if(x < 5) "Maybe"
else "No"</pre>
<h5>results</h5>
<pre>
"Maybe"
</pre>
<h5>example²</h5>
<pre>// Oxide also provides if - a ternary-operator-like if function

fact = n -> if(n <= 1, 1, n * fact(n - 1))
fact(6)</pre>
<h5>results</h5>
<pre>
720
</pre>
<hr>
<h4>▶️ Implicit Method Call</h4>
<h5>example¹</h5>
<pre>use durations
8:::hours()</pre>
<h5>results</h5>
<pre>
28800000
</pre>
<hr>
<h4>▶️ Import/Use</h4>
<h5>example¹</h5>
<pre>use tools
stocks = to_table([
   { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
   { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
   { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
])
stocks</pre>
<h5>results</h5>
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
<h4>▶️ Infix</h4>
<h5>example¹</h5>
<pre>let stock = { symbol: "TED", exchange: "AMEX", last_sale: 13.37 }
stock.last_sale</pre>
<h5>results</h5>
<pre>
13.37
</pre>
<hr>
<h4>▶️ Iteration</h4>
<h5>example¹</h5>
<pre>for row in tools::to_table(['apple', 'berry', 'kiwi', 'lime']) 
    yield row::value</pre>
<h5>results</h5>
<pre>
["apple", "berry", "kiwi", "lime"]
</pre>
<hr>
<h4>▶️ Match expression</h4>
<h5>example¹</h5>
<pre>let code = 100
match code {
   100 => "Accepted"
   n when n in 101..=104 => "Escalated"
   n when n < 100 => "Pending"
   n => "Rejected"
}</pre>
<h5>results</h5>
<pre>
"Accepted"
</pre>
<h5>example²</h5>
<pre>let code = 101
match code {
   100 => "Accepted"
   n when n in 101..=104 => "Escalated"
   n when n < 100 => "Pending"
   n => "Rejected"
}</pre>
<h5>results</h5>
<pre>
"Escalated"
</pre>
<h5>example³</h5>
<pre>let code = 99
match code {
   100 => "Accepted"
   n when n in 101..=104 => "Escalated"
   n when n < 100 => "Pending"
   n => "Rejected"
}</pre>
<h5>results</h5>
<pre>
"Pending"
</pre>
<h5>example⁴</h5>
<pre>let code = 110
match code {
   100 => "Accepted"
   n when n in 101..=104 => "Escalated"
   n when n < 100 => "Pending"
   n => "Rejected"
}</pre>
<h5>results</h5>
<pre>
"Rejected"
</pre>
<hr>
<h4>▶️ Mathematics: addition</h4>
<h5>example¹</h5>
<pre>5 + 6</pre>
<h5>results</h5>
<pre>
11
</pre>
<hr>
<h4>▶️ Mathematics: division</h4>
<h5>example¹</h5>
<pre>20.0 / 3</pre>
<h5>results</h5>
<pre>
6.666666666666667
</pre>
<hr>
<h4>▶️ Mathematics: multiplication</h4>
<h5>example¹</h5>
<pre>5 * 6</pre>
<h5>results</h5>
<pre>
30
</pre>
<hr>
<h4>▶️ Mathematics: subtraction</h4>
<h5>example¹</h5>
<pre>188 - 36</pre>
<h5>results</h5>
<pre>
152
</pre>
<hr>
<h4>▶️ Method Call</h4>
<h5>example¹</h5>
<pre>tools::to_table([
    'apple', 'berry', 'kiwi', 'lime'
])</pre>
<h5>results</h5>
<pre>
|------------|
| id | value |
|------------|
| 0  | apple |
| 1  | berry |
| 2  | kiwi  |
| 3  | lime  |
|------------|
</pre>
<hr>
<h4>▶️ Negative</h4>
<h5>example¹</h5>
<pre>let i = 75
let j = -i
j</pre>
<h5>results</h5>
<pre>
-75
</pre>
<hr>
<h4>▶️ Query</h4>
<h5>example¹</h5>
<pre>stocks = tools::to_table([
   { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
   { symbol: "GRU", exchange: "NYSE", last_sale: 56.88 },
   { symbol: "APK", exchange: "NASDAQ", last_sale: 32.12 }
])
from stocks where last_sale > 20.0</pre>
<h5>results</h5>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 1  | GRU    | NYSE     | 56.88     |
| 2  | APK    | NASDAQ   | 32.12     |
|------------------------------------|
</pre>
<hr>
<h4>▶️ Ranges</h4>
<h5>example¹</h5>
<pre>// Ranges may be exclusive

range = 1..5
tools::reverse(range)</pre>
<h5>results</h5>
<pre>
[4, 3, 2, 1]
</pre>
<h5>example²</h5>
<pre>// Ranges may be inclusive

range = 1..=5
tools::reverse(range)</pre>
<h5>results</h5>
<pre>
[5, 4, 3, 2, 1]
</pre>
<hr>
<h4>▶️ Testing</h4>
<h5>example¹</h5>
<pre>Feature "Matches function" {
    Scenario "Compare Array contents: Equal" {
        assert(
            [ 1 "a" "b" "c" ] matches [ 1 "a" "b" "c" ]
        )
    }
    Scenario "Compare Array contents: Not Equal" {
        assert(!(
            [ 1 "a" "b" "c" ] matches [ 0 "x" "y" "z" ]
        ))
    }
    Scenario "Compare JSON contents (in sequence)" {
        assert(
           { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
        )
    }
    Scenario "Compare JSON contents (out of sequence)" {
        assert(
           { scores: [82 78 99], id: "A1537" } 
                       matches 
           { id: "A1537", scores: [82 78 99] }
        )
    }
}</pre>
<h5>results</h5>
<pre>
|------------------------------------------------------------------------------------------------------------------------|
| id | level | item                                                                                    | passed | result |
|------------------------------------------------------------------------------------------------------------------------|
| 0  | 0     | Matches function                                                                        | true   | true   |
| 1  | 1     | Compare Array contents: Equal                                                           | true   | true   |
| 2  | 2     | assert [1, "a", "b", "c"] matches [1, "a", "b", "c"]                                    | true   | true   |
| 3  | 1     | Compare Array contents: Not Equal                                                       | true   | true   |
| 4  | 2     | assert !([1, "a", "b", "c"] matches [0, "x", "y", "z"])                                 | true   | true   |
| 5  | 1     | Compare JSON contents (in sequence)                                                     | true   | true   |
| 6  | 2     | assert {first: "Tom", last: "Lane"} matches {first: "Tom", last: "Lane"}                | true   | true   |
| 7  | 1     | Compare JSON contents (out of sequence)                                                 | true   | true   |
| 8  | 2     | assert {scores: [82, 78, 99], id: "A1537"} matches {id: "A1537", scores: [82, 78, 99]}  | true   | true   |
|------------------------------------------------------------------------------------------------------------------------|
</pre>
<hr>
<h4>▶️ Throw</h4>
<h5>example¹</h5>
<pre>throw("this is an error")</pre>
<h5>results</h5>
<pre>
this is an error
</pre>
<hr>
<h4>▶️ Tuples</h4>
<h5>example¹</h5>
<pre>// Tuples may be destructured to assign multiple variables

(a, b, c) = (3, 5, 7)
a + b + c</pre>
<h5>results</h5>
<pre>
15
</pre>
<h5>example²</h5>
<pre>// Tuples support addition

let a = (2, 4, 6)
let b = (1, 2, 3)
a + b</pre>
<h5>results</h5>
<pre>
(3, 6, 9)
</pre>
<h5>example³</h5>
<pre>// Tuples support subtraction

let a = (3, 5, 7)
let b = (1, 0, 1)
a - b</pre>
<h5>results</h5>
<pre>
(2, 5, 6)
</pre>
<h5>example⁴</h5>
<pre>// Tuples support negation

-(3, 6, 9)</pre>
<h5>results</h5>
<pre>
(-3, -6, -9)
</pre>
<h5>example⁵</h5>
<pre>// Tuples support multiplication

let a = (3, 5, 7)
let b = (1, 0, 1)
a * b</pre>
<h5>results</h5>
<pre>
(3, 0, 7)
</pre>
<h5>example⁶</h5>
<pre>// Tuples support division

let a = (3.0, 5.0, 9.0)
let b = (1.0, 2.0, 1.0)
a / b</pre>
<h5>results</h5>
<pre>
(3, 2.5, 9)
</pre>
<h5>example⁷</h5>
<pre>// Tuples support modulus

let a = (3.0, 5.0, 9.0)
let b = (1.0, 2.0, 1.0)
a % b</pre>
<h5>results</h5>
<pre>
(0.0, 1, 0.0)
</pre>
<h5>example⁸</h5>
<pre>// Tuples support exponents

let a = (2, 4, 6)
let b = (1, 2, 3)
a ** b</pre>
<h5>results</h5>
<pre>
(2, 16, 216)
</pre>
<hr>
<h4>▶️ Type Definitions</h4>
<h5>example¹</h5>
<pre>LabelString = typedef(String(80))
LabelString</pre>
<h5>results</h5>
<pre>
String(80)
</pre>
<hr>
<h4>▶️ Via Clause</h4>
<h5>example¹</h5>
<pre>stocks = nsd::save(
   "readme.via.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
rows = [
   { symbol: "ABCQ", exchange: "AMEX", last_sale: 12.49 },
   { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
   { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
]
rows ~> stocks

overwrite stocks via {symbol: "ABC", exchange: "NYSE", last_sale: 0.2308}
where symbol is "ABCQ"

from stocks</pre>
<h5>results</h5>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 0  | ABC    | NYSE     | 0.2308    |
| 1  | BOOM   | NYSE     | 56.88     |
| 2  | JET    | NASDAQ   | 32.12     |
|------------------------------------|
</pre>
<hr>
<h4>▶️ When statement</h4>
<h5>example¹</h5>
<pre>// Executes the block at the moment the condition becomes true.
let (x, y) = (1, 0)
whenever x == 0 {
    x = x + 1
    y = y + 1
}
x = x - 1
x + y</pre>
<h5>results</h5>
<pre>
2
</pre>
<h5>example²</h5>
<pre>// The block will not be executed if the condition is already true.
let (x, y) = (1, 0)
whenever x == 0 || y == 0 {
    x = x + 1
    y = y + 1
}
x + y</pre>
<h5>results</h5>
<pre>
1
</pre>
<h5>example³</h5>
<pre>// The block will be executed after the second assignment.
let (x, y) = (1, 0)
whenever x == 0 || y == 0 {
    x = x + 1
    y = y + 1
}
let (x, y) = (2, 3)
x + y</pre>
<h5>results</h5>
<pre>
5
</pre>
<hr>
<h4>▶️ While expression</h4>
<h5>example¹</h5>
<pre>let x = 0
while (x < 5) {
    x = x + 1
    yield x * 2
}</pre>
<h5>results</h5>
<pre>
[2, 4, 6, 8, 10]
</pre>
<hr>
<h4>▶️ Yield</h4>
<h5>example¹</h5>
<pre>for(i = 0, i < 5, i = i + 1) yield i * 2</pre>
<h5>results</h5>
<pre>
[0, 2, 4, 6, 8]
</pre>

<a name="platform_examples"></a>
### 📦 Platform Examples
    
<hr>
<h4>📦 arrays::filter &#8212; Filters an array based on a function</h4>
<h5>example1</h5>
<pre>arrays::filter(1..7, n -> (n % 2) == 0)</pre>
<h5>results</h5>
<pre>
[2, 4, 6]
</pre>
<hr>
<h4>📦 arrays::len &#8212; Returns the length of an array</h4>
<h5>example1</h5>
<pre>arrays::len([1, 5, 2, 4, 6, 0])</pre>
<h5>results</h5>
<pre>
6
</pre>
<hr>
<h4>📦 arrays::map &#8212; Transform an array based on a function</h4>
<h5>example1</h5>
<pre>arrays::map([1, 2, 3], n -> n * 2)</pre>
<h5>results</h5>
<pre>
[2, 4, 6]
</pre>
<hr>
<h4>📦 arrays::pop &#8212; Removes and returns a value or object from an array</h4>
<h5>example1</h5>
<pre>use arrays
stocks = []
stocks:::push({ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 })
stocks:::push({ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 })
stocks</pre>
<h5>results</h5>
<pre>
[]
</pre>
<hr>
<h4>📦 arrays::push &#8212; Appends a value or object to an array</h4>
<h5>example1</h5>
<pre>use arrays
stocks = [
    { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
    { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
    { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
]
stocks:::push({ symbol: "DEX", exchange: "OTC_BB", last_sale: 0.0086 })
from stocks</pre>
<h5>results</h5>
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
<h4>📦 arrays::reduce &#8212; Reduces an array to a single value</h4>
<h5>example1</h5>
<pre>arrays::reduce(1..=5, 0, (a, b) -> a + b)</pre>
<h5>results</h5>
<pre>
15
</pre>
<hr>
<h4>📦 arrays::reduce &#8212; Reduces an array to a single value</h4>
<h5>example2</h5>
<pre>use arrays::reduce
numbers = [1, 2, 3, 4, 5]
numbers:::reduce(0, (a, b) -> a + b)</pre>
<h5>results</h5>
<pre>
15
</pre>
<hr>
<h4>📦 arrays::reverse &#8212; Returns a reverse copy of an array</h4>
<h5>example1</h5>
<pre>arrays::reverse(['cat', 'dog', 'ferret', 'mouse'])</pre>
<h5>results</h5>
<pre>
["mouse", "ferret", "dog", "cat"]
</pre>
<hr>
<h4>📦 arrays::to_array &#8212; Converts a collection into an array</h4>
<h5>example1</h5>
<pre>arrays::to_array(tools::to_table([
   { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
   { symbol: "DMX", exchange: "OTC_BB", last_sale: 1.17 }
]))</pre>
<h5>results</h5>
<pre>
[{"exchange":"NYSE","last_sale":23.66,"symbol":"BIZ"}, {"exchange":"NYSE","last_sale":23.66,"symbol":"BIZ"}]
</pre>
<hr>
<h4>📦 cal::day_of &#8212; Returns the day of the month of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::day_of()</pre>
<h5>results</h5>
<pre>
5
</pre>
<hr>
<h4>📦 cal::hour12 &#8212; Returns the hour of the day of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::hour12()</pre>
<h5>results</h5>
<pre>
3
</pre>
<hr>
<h4>📦 cal::hour24 &#8212; Returns the hour (military time) of the day of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::hour24()</pre>
<h5>results</h5>
<pre>
15
</pre>
<hr>
<h4>📦 cal::minute_of &#8212; Returns the minute of the hour of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::minute_of()</pre>
<h5>results</h5>
<pre>
6
</pre>
<hr>
<h4>📦 cal::month_of &#8212; Returns the month of the year of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::month_of()</pre>
<h5>results</h5>
<pre>
6
</pre>
<hr>
<h4>📦 cal::second_of &#8212; Returns the seconds of the minute of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::second_of()</pre>
<h5>results</h5>
<pre>
1
</pre>
<hr>
<h4>📦 cal::year_of &#8212; Returns the year of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::year_of()</pre>
<h5>results</h5>
<pre>
2025
</pre>
<hr>
<h4>📦 cal::minus &#8212; Subtracts a duration from a date</h4>
<h5>example1</h5>
<pre>use cal, durations
cal::minus(now(), 3:::days())</pre>
<h5>results</h5>
<pre>
1969-12-29T00:00:00.000Z
</pre>
<hr>
<h4>📦 cal::now &#8212; Returns the current local date and time</h4>
<h5>example1</h5>
<pre>cal::now()</pre>
<h5>results</h5>
<pre>
2025-06-05T22:06:01.989Z
</pre>
<hr>
<h4>📦 cal::plus &#8212; Adds a duration to a date</h4>
<h5>example1</h5>
<pre>use cal, durations
cal::plus(now(), 30:::days())</pre>
<h5>results</h5>
<pre>
1970-01-31T00:00:00.000Z
</pre>
<hr>
<h4>📦 durations::days &#8212; Converts a number into the equivalent number of days</h4>
<h5>example1</h5>
<pre>use durations
3:::days()</pre>
<h5>results</h5>
<pre>
259200000
</pre>
<hr>
<h4>📦 durations::hours &#8212; Converts a number into the equivalent number of hours</h4>
<h5>example1</h5>
<pre>use durations
8:::hours()</pre>
<h5>results</h5>
<pre>
28800000
</pre>
<hr>
<h4>📦 durations::millis &#8212; Converts a number into the equivalent number of millis</h4>
<h5>example1</h5>
<pre>use durations
8:::millis()</pre>
<h5>results</h5>
<pre>
8
</pre>
<hr>
<h4>📦 durations::minutes &#8212; Converts a number into the equivalent number of minutes</h4>
<h5>example1</h5>
<pre>use durations
30:::minutes()</pre>
<h5>results</h5>
<pre>
1800000
</pre>
<hr>
<h4>📦 durations::seconds &#8212; Converts a number into the equivalent number of seconds</h4>
<h5>example1</h5>
<pre>use durations
30:::seconds()</pre>
<h5>results</h5>
<pre>
30000
</pre>
<hr>
<h4>📦 io::create_file &#8212; Creates a new file</h4>
<h5>example1</h5>
<pre>io::create_file("quote.json", {
   symbol: "TRX",
   exchange: "NYSE",
   last_sale: 45.32
})</pre>
<h5>results</h5>
<pre>
52
</pre>
<hr>
<h4>📦 io::exists &#8212; Returns true if the source path exists</h4>
<h5>example1</h5>
<pre>io::exists("quote.json")</pre>
<h5>results</h5>
<pre>
true
</pre>
<hr>
<h4>📦 io::read_text_file &#8212; Reads the contents of a text file into memory</h4>
<h5>example1</h5>
<pre>use io, util
file = "temp_secret.txt"
file:::create_file(md5("**keep**this**secret**"))
file:::read_text_file()</pre>
<h5>results</h5>
<pre>
"47338bd5f35bbb239092c36e30775b4a"
</pre>
<hr>
<h4>📦 io::stderr &#8212; Writes a string to STDERR</h4>
<h5>example1</h5>
<pre>io::stderr("Goodbye Cruel World")</pre>
<h5>results</h5>
<pre>
true
</pre>
<hr>
<h4>📦 io::stdout &#8212; Writes a string to STDOUT</h4>
<h5>example1</h5>
<pre>io::stdout("Hello World")</pre>
<h5>results</h5>
<pre>
true
</pre>
<hr>
<h4>📦 math::abs &#8212; abs(x): Returns the absolute value of x.</h4>
<h5>example1</h5>
<pre>math::abs(-81)</pre>
<h5>results</h5>
<pre>
81
</pre>
<hr>
<h4>📦 math::ceil &#8212; ceil(x): Returns the smallest integer greater than or equal to x.</h4>
<h5>example1</h5>
<pre>math::ceil(5.7)</pre>
<h5>results</h5>
<pre>
6
</pre>
<hr>
<h4>📦 math::floor &#8212; floor(x): Returns the largest integer less than or equal to x.</h4>
<h5>example1</h5>
<pre>math::floor(5.7)</pre>
<h5>results</h5>
<pre>
5
</pre>
<hr>
<h4>📦 math::max &#8212; max(a, b): Returns the larger of a and b</h4>
<h5>example1</h5>
<pre>math::max(81, 78)</pre>
<h5>results</h5>
<pre>
81
</pre>
<hr>
<h4>📦 math::min &#8212; min(a, b): Returns the smaller of a and b.</h4>
<h5>example1</h5>
<pre>math::min(81, 78)</pre>
<h5>results</h5>
<pre>
78
</pre>
<hr>
<h4>📦 math::pow &#8212; pow(x, y): Returns x raised to the power of y.</h4>
<h5>example1</h5>
<pre>math::pow(2, 3)</pre>
<h5>results</h5>
<pre>
8
</pre>
<hr>
<h4>📦 math::round &#8212; round(x): Rounds x to the nearest integer.</h4>
<h5>example1</h5>
<pre>math::round(5.3)</pre>
<h5>results</h5>
<pre>
5
</pre>
<hr>
<h4>📦 math::sqrt &#8212; sqrt(x): Returns the square root of x.</h4>
<h5>example1</h5>
<pre>math::sqrt(25)</pre>
<h5>results</h5>
<pre>
5
</pre>
<hr>
<h4>📦 nsd::create_event_src &#8212; Creates a journaled event-source</h4>
<h5>example1</h5>
<pre>nsd::create_event_src(
   "examples.event_src.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)</pre>
<h5>results</h5>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
|------------------------------------|
</pre>
<hr>
<h4>📦 nsd::create_fn &#8212; Creates a journaled table function</h4>
<h5>example1</h5>
<pre>nsd::create_fn(
   "examples.table_fn.stocks",
   (symbol: String(8), exchange: String(8), last_sale: f64) -> {
       symbol: symbol,
       exchange: exchange,
       last_sale: last_sale * 2.0,
       event_time: cal::now()
   })</pre>
<h5>results</h5>
<pre>
|-------------------------------------------------|
| id | symbol | exchange | last_sale | event_time |
|-------------------------------------------------|
|-------------------------------------------------|
</pre>
<hr>
<h4>📦 nsd::drop &#8212; Deletes a dataframe from a namespace</h4>
<h5>example1</h5>
<pre>nsd::save('packages.remove.stocks', Table::new(
    symbol: String(8),
    exchange: String(8),
    last_sale: f64
))

nsd::drop('packages.remove.stocks')
nsd::exists('packages.remove.stocks')</pre>
<h5>results</h5>
<pre>
false
</pre>
<hr>
<h4>📦 nsd::exists &#8212; Returns true if the source path exists</h4>
<h5>example1</h5>
<pre>nsd::save('packages.exists.stocks', Table::new(
   symbol: String(8),
   exchange: String(8),
   last_sale: f64
))
nsd::exists("packages.exists.stocks")</pre>
<h5>results</h5>
<pre>
true
</pre>
<hr>
<h4>📦 nsd::exists &#8212; Returns true if the source path exists</h4>
<h5>example2</h5>
<pre>nsd::exists("packages.not_exists.stocks")</pre>
<h5>results</h5>
<pre>
false
</pre>
<hr>
<h4>📦 nsd::journal &#8212; Retrieves the journal for an event-source or table function</h4>
<h5>example1</h5>
<pre>use nsd
nsd::drop("examples.journal.stocks");
stocks = nsd::create_fn(
   "examples.journal.stocks",
   (symbol: String(8), exchange: String(8), last_sale: f64) -> {
       symbol: symbol,
       exchange: exchange,
       last_sale: last_sale * 2.0,
       ingest_time: cal::now()
   });
[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
stocks:::journal()</pre>
<h5>results</h5>
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
<h4>📦 nsd::load &#8212; Loads a dataframe from a namespace</h4>
<h5>example1</h5>
<pre>let stocks =
   nsd::save('packages.save_load.stocks', Table::new(
       symbol: String(8),
       exchange: String(8),
       last_sale: f64
   ))

let rows = 
   [{ symbol: "CAZ", exchange: "AMEX", last_sale: 65.13 },
    { symbol: "BAL", exchange: "NYSE", last_sale: 82.78 },
    { symbol: "RCE", exchange: "NASDAQ", last_sale: 124.09 }] 

rows ~> stocks

nsd::load('packages.save_load.stocks')</pre>
<h5>results</h5>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 0  | CAZ    | AMEX     | 65.13     |
| 1  | BAL    | NYSE     | 82.78     |
| 2  | RCE    | NASDAQ   | 124.09    |
|------------------------------------|
</pre>
<hr>
<h4>📦 nsd::replay &#8212; Reconstructs the state of a journaled table</h4>
<h5>example1</h5>
<pre>use nsd
nsd::drop("examples.replay.stocks");
stocks = nsd::create_fn(
   "examples.replay.stocks",
   (symbol: String(8), exchange: String(8), last_sale: f64) -> {
       symbol: symbol,
       exchange: exchange,
       last_sale: last_sale * 2.0,
       rank: __row_id__ + 1
   });
[{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
stocks:::replay()</pre>
<h5>results</h5>
<pre>
3
</pre>
<hr>
<h4>📦 nsd::save &#8212; Creates a new dataframe</h4>
<h5>example1</h5>
<pre>let stocks =
   nsd::save('packages.save.stocks', Table::new(
       symbol: String(8),
       exchange: String(8),
       last_sale: f64
   ))

let rows = 
   [{ symbol: "TCO", exchange: "NYSE", last_sale: 38.53 },
    { symbol: "SHMN", exchange: "NYSE", last_sale: 6.57 },
    { symbol: "HMU", exchange: "NASDAQ", last_sale: 27.12 }] 

rows ~> stocks
stocks</pre>
<h5>results</h5>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 0  | TCO    | NYSE     | 38.53     |
| 1  | SHMN   | NYSE     | 6.57      |
| 2  | HMU    | NASDAQ   | 27.12     |
|------------------------------------|
</pre>
<hr>
<h4>📦 os::call &#8212; Invokes an operating system application</h4>
<h5>example1</h5>
<pre>stocks = nsd::save(
   "examples.os.call",
    Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
os::call("chmod", "777", oxide::home())</pre>
<h5>results</h5>
<pre>
""
</pre>
<hr>
<h4>📦 os::clear &#8212; Clears the terminal/screen</h4>
<h5>example1</h5>
<pre>os::clear()</pre>
<h5>results</h5>
<pre>
true
</pre>
<hr>
<h4>📦 os::current_dir &#8212; Returns the current directory</h4>
<h5>example1</h5>
<pre>use str
cur_dir = os::current_dir()
prefix = if(cur_dir:::ends_with("core"), "../..", ".")
path_str = prefix + "/demoes/language/include_file.oxide"
include path_str</pre>
<h5>results</h5>
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
<h4>📦 os::env &#8212; Returns a table of the OS environment variables</h4>
<h5>example1</h5>
<pre>os::env()</pre>
<h5>results</h5>
<pre>
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id | key                        | value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 0  | BUN_INSTALL                | /Users/ldaniels/.bun                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 1  | CARGO                      | /Users/ldaniels/.rustup/toolchains/stable-aarch64-apple-darwin/bin/cargo                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 2  | CARGO_HOME                 | /Users/ldaniels/.cargo                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 3  | CARGO_MANIFEST_DIR         | /Users/ldaniels/GitHub/oxide/src/core                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 4  | CARGO_MANIFEST_PATH        | /Users/ldaniels/GitHub/oxide/src/core/Cargo.toml                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 5  | CARGO_PKG_AUTHORS          |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 6  | CARGO_PKG_DESCRIPTION      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 7  | CARGO_PKG_HOMEPAGE         |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 8  | CARGO_PKG_LICENSE          |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 9  | CARGO_PKG_LICENSE_FILE     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 10 | CARGO_PKG_NAME             | core                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 11 | CARGO_PKG_README           |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 12 | CARGO_PKG_REPOSITORY       |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 13 | CARGO_PKG_RUST_VERSION     |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 14 | CARGO_PKG_VERSION          | 0.1.0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 15 | CARGO_PKG_VERSION_MAJOR    | 0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 16 | CARGO_PKG_VERSION_MINOR    | 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 17 | CARGO_PKG_VERSION_PATCH    | 0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 18 | CARGO_PKG_VERSION_PRE      |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 19 | COMMAND_MODE               | unix2003                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 20 | DYLD_FALLBACK_LIBRARY_PATH | /Users/ldaniels/GitHub/oxide/target/debug/build/curl-sys-976ef1fd41b2ae67/out/build:/Users/ldaniels/GitHub/oxide/target/debug/build/libnghttp2-sys-03d0e22189823925/out/i/lib:/Users/ldaniels/GitHub/oxide/target/debug/build/zstd-sys-b2743e594d963e4d/out:/Users/ldaniels/GitHub/oxide/target/debug/deps:/Users/ldaniels/GitHub/oxide/target/debug:/Users/ldaniels/.rustup/toolchains/stable-aarch64-apple-darwin/lib/rustlib/aarch64-apple-darwin/lib:/Users/ldaniels/.rustup/toolchains/stable-aarch64-apple-darwin/lib:/Users/ldaniels/lib:/usr/local/lib:/usr/lib |
| 21 | HOME                       | /Users/ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 22 | JAVA_HOME                  | /Users/ldaniels/.sdkman/candidates/java/current                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 23 | LC_CTYPE                   | en_US.UTF-8                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 24 | LOGNAME                    | ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 25 | OLDPWD                     | /                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 26 | PATH                       | /Users/ldaniels/.bun/bin:/Users/ldaniels/.sdkman/candidates/java/current/bin:/usr/local/bin:/System/Cryptexes/App/usr/bin:/usr/bin:/bin:/usr/sbin:/sbin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/local/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/appleinternal/bin:/Users/ldaniels/.cargo/bin:/opt/homebrew/bin:.                                                                                                                                  |
| 27 | PWD                        | /Users/ldaniels/GitHub/oxide                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 28 | RR_REAL_RUSTDOC            | /Users/ldaniels/.cargo/bin/rustdoc                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 29 | RUSTC                      | /Users/ldaniels/.cargo/bin/rustc                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 30 | RUSTC_BOOTSTRAP            | 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 31 | RUSTDOC                    | /Users/ldaniels/Library/Application Support/JetBrains/IntelliJIdea2025.1/plugins/intellij-rust/bin/mac/aarch64/intellij-rust-native-helper                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 32 | RUSTUP_HOME                | /Users/ldaniels/.rustup                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 33 | RUSTUP_TOOLCHAIN           | stable-aarch64-apple-darwin                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 34 | RUST_BACKTRACE             | short                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 35 | RUST_RECURSION_COUNT       | 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 36 | SDKMAN_CANDIDATES_API      | https://api.sdkman.io/2                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 37 | SDKMAN_CANDIDATES_DIR      | /Users/ldaniels/.sdkman/candidates                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 38 | SDKMAN_DIR                 | /Users/ldaniels/.sdkman                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 39 | SDKMAN_PLATFORM            | darwinarm64                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 40 | SHELL                      | /bin/zsh                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 41 | SSH_AUTH_SOCK              | /private/tmp/com.apple.launchd.lSmiPPAhaN/Listeners                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 42 | TERM                       | ansi                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 43 | TMPDIR                     | /var/folders/ld/hwrvzn011w79gftyb6vj8mg40000gn/T/                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 44 | USER                       | ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 45 | XPC_FLAGS                  | 0x0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 46 | XPC_SERVICE_NAME           | application.com.jetbrains.intellij.505803.104316344                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 47 | __CFBundleIdentifier       | com.jetbrains.intellij                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 48 | __CF_USER_TEXT_ENCODING    | 0x1F5:0x0:0x0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
</pre>
<hr>
<h4>📦 oxide::compile &#8212; Compiles source code from a string input</h4>
<h5>example1</h5>
<pre>code = oxide::compile("2 ** 4")
code()</pre>
<h5>results</h5>
<pre>
16
</pre>
<hr>
<h4>📦 oxide::debug &#8212; Compiles source code from a string input; returning a debug string</h4>
<h5>example1</h5>
<pre>oxide::debug("2 ** 4")</pre>
<h5>results</h5>
<pre>
"Ok(Pow(Literal(Number(I64Value(2))), Literal(Number(I64Value(4)))))"
</pre>
<hr>
<h4>📦 oxide::eval &#8212; Evaluates a string containing Oxide code</h4>
<h5>example1</h5>
<pre>a = 'Hello '
b = 'World'
oxide::eval("a + b")</pre>
<h5>results</h5>
<pre>
"Hello World"
</pre>
<hr>
<h4>📦 oxide::help &#8212; Integrated help function</h4>
<h5>example1</h5>
<pre>from oxide::help() limit 3</pre>
<h5>results</h5>
<pre>
|-------------------------------------------------------------------------------------------------------------------------------------|
| id | name        | module | signature                                 | description                                       | returns |
|-------------------------------------------------------------------------------------------------------------------------------------|
| 0  | to_string   | str    | str::to_string(a)                         | Converts a value to its text-based representation | String  |
| 1  | superscript | str    | str::superscript(n: i64)                  | Returns a superscript of a number `n`             | String  |
| 2  | substring   | str    | str::substring(s: String, m: i64, n: i64) | Returns a substring of string `s` from `m` to `n` | String  |
|-------------------------------------------------------------------------------------------------------------------------------------|
</pre>
<hr>
<h4>📦 oxide::home &#8212; Returns the Oxide home directory path</h4>
<h5>example1</h5>
<pre>oxide::home()</pre>
<h5>results</h5>
<pre>
"/Users/ldaniels/oxide_db"
</pre>
<hr>
<h4>📦 oxide::inspect &#8212; Returns a table describing the structure of a model</h4>
<h5>example1</h5>
<pre>oxide::inspect("{ x = 1 x = x + 1 }")</pre>
<h5>results</h5>
<pre>
|-------------------------------------------------------------------------------------------------|
| id | code      | model                                                                          |
|-------------------------------------------------------------------------------------------------|
| 0  | x = 1     | SetVariables(Variable("x"), Literal(Number(I64Value(1))))                      |
| 1  | x = x + 1 | SetVariables(Variable("x"), Plus(Variable("x"), Literal(Number(I64Value(1))))) |
|-------------------------------------------------------------------------------------------------|
</pre>
<hr>
<h4>📦 oxide::inspect &#8212; Returns a table describing the structure of a model</h4>
<h5>example2</h5>
<pre>oxide::inspect("stock::is_this_you('ABC')")</pre>
<h5>results</h5>
<pre>
|---------------------------------------------------------------------------------------------------------------------------------------------------|
| id | code                      | model                                                                                                            |
|---------------------------------------------------------------------------------------------------------------------------------------------------|
| 0  | stock::is_this_you("ABC") | ColonColon(Variable("stock"), FunctionCall { fx: Variable("is_this_you"), args: [Literal(StringValue("ABC"))] }) |
|---------------------------------------------------------------------------------------------------------------------------------------------------|
</pre>
<hr>
<h4>📦 oxide::printf &#8212; C-style "printf" function</h4>
<h5>example1</h5>
<pre>oxide::printf("Hello %s", "World")</pre>
<h5>results</h5>
<pre>
true
</pre>
<hr>
<h4>📦 oxide::println &#8212; Print line function</h4>
<h5>example1</h5>
<pre>oxide::println("Hello World")</pre>
<h5>results</h5>
<pre>
true
</pre>
<hr>
<h4>📦 oxide::reset &#8212; Clears the scope of all user-defined objects</h4>
<h5>example1</h5>
<pre>oxide::reset()</pre>
<h5>results</h5>
<pre>
true
</pre>
<hr>
<h4>📦 oxide::sprintf &#8212; C-style "sprintf" function</h4>
<h5>example1</h5>
<pre>oxide::sprintf("Hello %s", "World")</pre>
<h5>results</h5>
<pre>
"Hello World"
</pre>
<hr>
<h4>📦 oxide::uuid &#8212; Returns a random 128-bit UUID</h4>
<h5>example1</h5>
<pre>oxide::uuid()</pre>
<h5>results</h5>
<pre>
422e9dd4-dabb-42c3-ae66-8abfb61c1a13
</pre>
<hr>
<h4>📦 oxide::version &#8212; Returns the Oxide version</h4>
<h5>example1</h5>
<pre>oxide::version()</pre>
<h5>results</h5>
<pre>
"0.41"
</pre>
<hr>
<h4>📦 str::ends_with &#8212; Returns true if string `a` ends with string `b`</h4>
<h5>example1</h5>
<pre>str::ends_with('Hello World', 'World')</pre>
<h5>results</h5>
<pre>
true
</pre>
<hr>
<h4>📦 str::format &#8212; Returns an argument-formatted string</h4>
<h5>example1</h5>
<pre>str::format("This {} the {}", "is", "way")</pre>
<h5>results</h5>
<pre>
"This is the way"
</pre>
<hr>
<h4>📦 str::index_of &#8212; Returns the index of string `b` in string `a`</h4>
<h5>example1</h5>
<pre>str::index_of('The little brown fox', 'brown')</pre>
<h5>results</h5>
<pre>
11
</pre>
<hr>
<h4>📦 str::join &#8212; Combines an array into a string</h4>
<h5>example1</h5>
<pre>str::join(['1', 5, 9, '13'], ', ')</pre>
<h5>results</h5>
<pre>
"1, 5, 9, 13"
</pre>
<hr>
<h4>📦 str::left &#8212; Returns n-characters from left-to-right</h4>
<h5>example1</h5>
<pre>str::left('Hello World', 5)</pre>
<h5>results</h5>
<pre>
"Hello"
</pre>
<hr>
<h4>📦 str::len &#8212; Returns the number of characters contained in the string</h4>
<h5>example1</h5>
<pre>str::len('The little brown fox')</pre>
<h5>results</h5>
<pre>
20
</pre>
<hr>
<h4>📦 str::right &#8212; Returns n-characters from right-to-left</h4>
<h5>example1</h5>
<pre>str::right('Hello World', 5)</pre>
<h5>results</h5>
<pre>
"World"
</pre>
<hr>
<h4>📦 str::split &#8212; Splits string `a` by delimiter string `b`</h4>
<h5>example1</h5>
<pre>str::split('Hello,there World', ' ,')</pre>
<h5>results</h5>
<pre>
["Hello", "there", "World"]
</pre>
<hr>
<h4>📦 str::starts_with &#8212; Returns true if string `a` starts with string `b`</h4>
<h5>example1</h5>
<pre>str::starts_with('Hello World', 'World')</pre>
<h5>results</h5>
<pre>
false
</pre>
<hr>
<h4>📦 str::strip_margin &#8212; Returns the string with all characters on each line are striped up to the margin character</h4>
<h5>example1</h5>
<pre>str::strip_margin("
|Code example:
|
|from stocks
|where exchange is 'NYSE'
", '|')</pre>
<h5>results</h5>
<pre>
"
Code example:

from stocks
where exchange is 'NYSE'"
</pre>
<hr>
<h4>📦 str::substring &#8212; Returns a substring of string `s` from `m` to `n`</h4>
<h5>example1</h5>
<pre>str::substring('Hello World', 0, 5)</pre>
<h5>results</h5>
<pre>
"Hello"
</pre>
<hr>
<h4>📦 str::superscript &#8212; Returns a superscript of a number `n`</h4>
<h5>example1</h5>
<pre>str::superscript(5)</pre>
<h5>results</h5>
<pre>
"⁵"
</pre>
<hr>
<h4>📦 str::to_string &#8212; Converts a value to its text-based representation</h4>
<h5>example1</h5>
<pre>str::to_string(125.75)</pre>
<h5>results</h5>
<pre>
"125.75"
</pre>
<hr>
<h4>📦 tools::compact &#8212; Shrinks a table by removing deleted rows</h4>
<h5>example1</h5>
<pre>stocks = nsd::save(
   "examples.compact.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
[{ symbol: "DMX", exchange: "NYSE", last_sale: 99.99 },
 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
 { symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 },
 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
delete from stocks where last_sale > 1.0
from stocks</pre>
<h5>results</h5>
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
<h4>📦 tools::describe &#8212; Describes a table or structure</h4>
<h5>example1</h5>
<pre>tools::describe({
   symbol: "BIZ",
   exchange: "NYSE",
   last_sale: 23.66
})</pre>
<h5>results</h5>
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
<h4>📦 tools::describe &#8212; Describes a table or structure</h4>
<h5>example2</h5>
<pre>stocks =
    |--------------------------------------|
    | symbol | exchange | last_sale | rank |
    |--------------------------------------|
    | BOOM   | NYSE     | 113.76    | 1    |
    | ABC    | AMEX     | 24.98     | 2    |
    | JET    | NASDAQ   | 64.24     | 3    |
    |--------------------------------------|
tools::describe(stocks)</pre>
<h5>results</h5>
<pre>
|----------------------------------------------------------|
| id | name      | type      | default_value | is_nullable |
|----------------------------------------------------------|
| 0  | symbol    | String(4) | null          | true        |
| 1  | exchange  | String(6) | null          | true        |
| 2  | last_sale | f64       | null          | true        |
| 3  | rank      | i64       | null          | true        |
|----------------------------------------------------------|
</pre>
<hr>
<h4>📦 tools::fetch &#8212; Retrieves a raw structure from a table</h4>
<h5>example1</h5>
<pre>stocks = nsd::save(
   "examples.fetch.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
tools::fetch(stocks, 2)</pre>
<h5>results</h5>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 2  | JET    | NASDAQ   | 32.12     |
|------------------------------------|
</pre>
<hr>
<h4>📦 tools::filter &#8212; Filters a collection based on a function</h4>
<h5>example1</h5>
<pre>tools::filter(1..11, n -> (n % 2) == 0)</pre>
<h5>results</h5>
<pre>
[2, 4, 6, 8, 10]
</pre>
<hr>
<h4>📦 tools::len &#8212; Returns the length of a table</h4>
<h5>example1</h5>
<pre>stocks = nsd::save(
   "examples.table_len.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
[{ symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
 { symbol: "ACDC", exchange: "AMEX", last_sale: 35.11 },
 { symbol: "UELO", exchange: "NYSE", last_sale: 90.12 }] ~> stocks
tools::len(stocks)</pre>
<h5>results</h5>
<pre>
3
</pre>
<hr>
<h4>📦 tools::map &#8212; Transform a collection based on a function</h4>
<h5>example1</h5>
<pre>stocks = nsd::save(
   "examples.map_over_table.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
[{ symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
 { symbol: "ACDC", exchange: "AMEX", last_sale: 35.11 },
 { symbol: "UELO", exchange: "NYSE", last_sale: 90.12 }] ~> stocks
use tools
stocks:::map(row -> {
    symbol: symbol,
    exchange: exchange,
    last_sale: last_sale,
    processed_time: cal::now()
})</pre>
<h5>results</h5>
<pre>
|---------------------------------------------------------------|
| id | symbol | exchange | last_sale | processed_time           |
|---------------------------------------------------------------|
| 0  | WKRP   | NYSE     | 11.11     | 2025-06-05T22:06:02.456Z |
| 1  | ACDC   | AMEX     | 35.11     | 2025-06-05T22:06:02.456Z |
| 2  | UELO   | NYSE     | 90.12     | 2025-06-05T22:06:02.458Z |
|---------------------------------------------------------------|
</pre>
<hr>
<h4>📦 tools::pop &#8212; Removes and returns a value or object from a Sequence</h4>
<h5>example1</h5>
<pre>use tools
stocks = nsd::save(
   "examples.tools_pop.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
stocks:::pop()</pre>
<h5>results</h5>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 2  | JET    | NASDAQ   | 32.12     |
|------------------------------------|
</pre>
<hr>
<h4>📦 tools::push &#8212; Appends a value or object to a Sequence</h4>
<h5>example1</h5>
<pre>use tools
stocks = nsd::save(
   "examples.tools_push.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
stocks:::push({ symbol: "XYZ", exchange: "NASDAQ", last_sale: 24.78 })
stocks</pre>
<h5>results</h5>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 0  | ABC    | AMEX     | 12.49     |
| 1  | BOOM   | NYSE     | 56.88     |
| 2  | JET    | NASDAQ   | 32.12     |
| 3  | XYZ    | NASDAQ   | 24.78     |
|------------------------------------|
</pre>
<hr>
<h4>📦 tools::reverse &#8212; Returns a reverse copy of a table, string or array</h4>
<h5>example1</h5>
<pre>use tools
to_table(reverse(
   ['cat', 'dog', 'ferret', 'mouse']
))</pre>
<h5>results</h5>
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
<h4>📦 tools::row_id &#8212; Returns the unique ID for the last retrieved row</h4>
<h5>example1</h5>
<pre>tools::row_id()</pre>
<h5>results</h5>
<pre>
0
</pre>
<hr>
<h4>📦 tools::scan &#8212; Returns existence metadata for a table</h4>
<h5>example1</h5>
<pre>use tools
stocks = nsd::save(
   "examples.scan.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
 { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1442 },
 { symbol: "XYZ", exchange: "NYSE", last_sale: 0.0289 }] ~> stocks
delete from stocks where last_sale > 1.0
stocks:::scan()</pre>
<h5>results</h5>
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
<h4>📦 tools::to_array &#8212; Converts a collection into an array</h4>
<h5>example1</h5>
<pre>tools::to_array("Hello")</pre>
<h5>results</h5>
<pre>
["H", "e", "l", "l", "o"]
</pre>
<hr>
<h4>📦 tools::to_csv &#8212; Converts a collection to CSV format</h4>
<h5>example1</h5>
<pre>use tools::to_csv
stocks = nsd::save(
   "examples.csv.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
[{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
stocks:::to_csv()</pre>
<h5>results</h5>
<pre>
[""ABC","AMEX",11.11", ""UNO","OTC",0.2456", ""BIZ","NYSE",23.66", ""GOTO","OTC",0.1428", ""BOOM","NASDAQ",0.0872"]
</pre>
<hr>
<h4>📦 tools::to_json &#8212; Converts a collection to JSON format</h4>
<h5>example1</h5>
<pre>use tools::to_json
stocks = nsd::save(
   "examples.json.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
[{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
 { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
 { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
 { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
 { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
stocks:::to_json()</pre>
<h5>results</h5>
<pre>
["{"symbol":"ABC","exchange":"AMEX","last_sale":11.11}", "{"symbol":"UNO","exchange":"OTC","last_sale":0.2456}", "{"symbol":"BIZ","exchange":"NYSE","last_sale":23.66}", "{"symbol":"GOTO","exchange":"OTC","last_sale":0.1428}", "{"symbol":"BOOM","exchange":"NASDAQ","last_sale":0.0872}"]
</pre>
<hr>
<h4>📦 tools::to_table &#8212; Converts an object into a to_table</h4>
<h5>example1</h5>
<pre>tools::to_table(['cat', 'dog', 'ferret', 'mouse'])</pre>
<h5>results</h5>
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
<h4>📦 util::base64 &#8212; Translates bytes into Base 64</h4>
<h5>example1</h5>
<pre>util::base64('Hello World')</pre>
<h5>results</h5>
<pre>
"SGVsbG8gV29ybGQ="
</pre>
<hr>
<h4>📦 util::to_binary &#8212; Translates a numeric value into binary</h4>
<h5>example1</h5>
<pre>util::to_binary(0b1011 & 0b1101)</pre>
<h5>results</h5>
<pre>
"1001"
</pre>
<hr>
<h4>📦 util::gzip &#8212; Compresses bytes via gzip</h4>
<h5>example1</h5>
<pre>util::gzip('Hello World')</pre>
<h5>results</h5>
<pre>
1f8b08000000000000fff348cdc9c95708cf2fca49010056b1174a0b000000
</pre>
<hr>
<h4>📦 util::gunzip &#8212; Decompresses bytes via gzip</h4>
<h5>example1</h5>
<pre>util::gunzip(util::gzip('Hello World'))</pre>
<h5>results</h5>
<pre>
48656c6c6f20576f726c64
</pre>
<hr>
<h4>📦 util::hex &#8212; Translates bytes into hexadecimal</h4>
<h5>example1</h5>
<pre>util::hex('Hello World')</pre>
<h5>results</h5>
<pre>
"48656c6c6f20576f726c64"
</pre>
<hr>
<h4>📦 util::md5 &#8212; Creates a MD5 digest</h4>
<h5>example1</h5>
<pre>util::md5('Hello World')</pre>
<h5>results</h5>
<pre>
b10a8db164e0754105b7a99be72e3fe5
</pre>
<hr>
<h4>📦 util::to &#8212; Converts a value to the desired type</h4>
<h5>example1</h5>
<pre>util::to(1376438453123, Date)</pre>
<h5>results</h5>
<pre>
2013-08-14T00:00:53.123Z
</pre>
<hr>
<h4>📦 util::to_ascii &#8212; Converts an integer to ASCII</h4>
<h5>example1</h5>
<pre>util::to_ascii(177)</pre>
<h5>results</h5>
<pre>
"±"
</pre>
<hr>
<h4>📦 util::to_date &#8212; Converts a value to Date</h4>
<h5>example1</h5>
<pre>util::to_date(177)</pre>
<h5>results</h5>
<pre>
1970-01-01T00:00:00.177Z
</pre>
<hr>
<h4>📦 util::to_f64 &#8212; Converts a value to f64</h4>
<h5>example1</h5>
<pre>util::to_f64(4321)</pre>
<h5>results</h5>
<pre>
4321
</pre>
<hr>
<h4>📦 util::to_i64 &#8212; Converts a value to i64</h4>
<h5>example1</h5>
<pre>util::to_i64(88)</pre>
<h5>results</h5>
<pre>
88
</pre>
<hr>
<h4>📦 util::to_i128 &#8212; Converts a value to i128</h4>
<h5>example1</h5>
<pre>util::to_i128(88)</pre>
<h5>results</h5>
<pre>
88
</pre>
<hr>
<h4>📦 util::to_u128 &#8212; Converts a value to u128</h4>
<h5>example1</h5>
<pre>util::to_u128(88)</pre>
<h5>results</h5>
<pre>
88
</pre>
<hr>
<h4>📦 www::url_decode &#8212; Decodes a URL-encoded string</h4>
<h5>example1</h5>
<pre>www::url_decode('http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998')</pre>
<h5>results</h5>
<pre>
"http://shocktrade.com?name=the hero&t=9998"
</pre>
<hr>
<h4>📦 www::url_encode &#8212; Encodes a URL string</h4>
<h5>example1</h5>
<pre>www::url_encode('http://shocktrade.com?name=the hero&t=9998')</pre>
<h5>results</h5>
<pre>
"http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998"
</pre>
<hr>
<h4>📦 www::serve &#8212; Starts a local HTTP service</h4>
<h5>example1</h5>
<pre>www::serve(8787)
stocks = nsd::save(
   "examples.www.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
[{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 },
 { symbol: "BOX", exchange: "NYSE", last_sale: 56.88 },
 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
 { symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }] ~> stocks
GET http://localhost:8787/examples/www/quotes/1/4</pre>
<h5>results</h5>
<pre>
[{"exchange":"NYSE","last_sale":56.88,"symbol":"BOX"}, {"exchange":"NASDAQ","last_sale":32.12,"symbol":"JET"}, {"exchange":"AMEX","last_sale":12.49,"symbol":"ABC"}]
</pre>
