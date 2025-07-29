
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
Built-in modules like `io`, `math`, `http`, and more cover the essentials—without reaching for external libraries.

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
users = [ { name: 'Tom' }, { name: 'Sara' } ]
names = users::map(u -> u.name)
```

### 🕒 Work with Dates and Durations
```oxide
DateTime::new::plus(30::days)
```

### 🔄 Compose Data Pipelines
```oxide
let arr = [1, 2, 3, 4]
arr::filter(x -> (x % 2) == 0)::map(x -> x * 10)
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

- `io`, `math`, `os`, `oxide` and `http`.

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

## Development

#### Build the Oxide REPL and Server

```bash
cargo build --release
```

You'll find the executables in `./target/release/`:
* `oxide_repl` is the Oxide REST client / REPL
* `oxide_server` is the Oxide REST Server
