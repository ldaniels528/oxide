📖 Core Language Examples
========================================
<hr>
<h4>▶️ Aliases</h4>
<h5>example¹</h5>
<pre>name: 'Tom'</pre>
<h5>results</h5>
<pre>
"Tom"

</pre>
<h5>example²</h5>
<pre>{ name: 'Tom' }::to_table()</pre>
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

[1, 4, 2, 8, 5, 7]::reverse()</pre>
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
   Table(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: DateTime))::new
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
   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
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
<pre>fn inverse(a) -> 1.0 / a
result = 5 |> inverse
result</pre>
<h5>results</h5>
<pre>
0.2

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
   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
)
http::serve(8855)</pre>
<h5>results</h5>
<pre>
true

</pre>
<h5>example²</h5>
<pre>POST {
    url: http://localhost:8855/readme/www/stocks/0
    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }
}</pre>
<h5>results</h5>
<pre>
0

</pre>
<h5>example³</h5>
<pre>GET http://localhost:8855/readme/www/stocks/0</pre>
<h5>results</h5>
<pre>
{exchange: "AMEX", last_sale: 11.77, symbol: "ABC"}

</pre>
<h5>example⁴</h5>
<pre>HEAD http://localhost:8855/readme/www/stocks/0</pre>
<h5>results</h5>
<pre>
{content-length: "80", content-type: "application/json", date: "Fri, 11 Jul 2025 17:44:00 GMT"}

</pre>
<h5>example⁵</h5>
<pre>PUT {
    url: http://localhost:8855/readme/www/stocks/0
    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.79 }
}</pre>
<h5>results</h5>
<pre>
1

</pre>
<h5>example⁶</h5>
<pre>GET http://localhost:8855/readme/www/stocks/0</pre>
<h5>results</h5>
<pre>
{exchange: "AMEX", last_sale: 11.79, symbol: "ABC"}

</pre>
<h5>example⁷</h5>
<pre>PATCH {
    url: http://localhost:8855/readme/www/stocks/0
    body: { last_sale: 11.81 }
}</pre>
<h5>results</h5>
<pre>
1

</pre>
<h5>example⁸</h5>
<pre>GET http://localhost:8855/readme/www/stocks/0</pre>
<h5>results</h5>
<pre>
{exchange: "AMEX", last_sale: 11.81, symbol: "ABC"}

</pre>
<h5>example⁹</h5>
<pre>DELETE http://localhost:8855/readme/www/stocks/0</pre>
<h5>results</h5>
<pre>
1

</pre>
<h5>example¹⁰</h5>
<pre>GET http://localhost:8855/readme/www/stocks/0</pre>
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
<pre>8::hours()</pre>
<h5>results</h5>
<pre>
28800000

</pre>
<hr>
<h4>▶️ Import/Use</h4>
<h5>example¹</h5>
<pre>stocks = [
   { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
   { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
   { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
]::to_table()
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
<h4>▶️ Is Defined</h4>
<h5>example¹</h5>
<pre>let stock = { symbol: "TED", exchange: "AMEX", last_sale: 13.37 }
is_defined(stock)</pre>
<h5>results</h5>
<pre>
true

</pre>
<h5>example²</h5>
<pre>is_defined(x)</pre>
<h5>results</h5>
<pre>
false

</pre>
<hr>
<h4>▶️ Iteration</h4>
<h5>example¹</h5>
<pre>for row in ['apple', 'berry', 'kiwi', 'lime']::to_table() 
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
<pre>['apple', 'berry', 'kiwi', 'lime']::to_table()</pre>
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
<h4>▶️ Ranges</h4>
<h5>example¹</h5>
<pre>// Ranges may be exclusive

range = 1..5
range::reverse()</pre>
<h5>results</h5>
<pre>
[4, 3, 2, 1]

</pre>
<h5>example²</h5>
<pre>// Ranges may be inclusive

range = 1..=5
range::reverse()</pre>
<h5>results</h5>
<pre>
[5, 4, 3, 2, 1]

</pre>
<hr>
<h4>▶️ Testing</h4>
<h5>example¹</h5>
<pre>feature "Matches function" {
    scenario "Compare Array contents: Equal" {
        assert(
            [ 1 "a" "b" "c" ] matches [ 1 "a" "b" "c" ]
        )
    }
    scenario "Compare Array contents: Not Equal" {
        assert(!(
            [ 1 "a" "b" "c" ] matches [ 0 "x" "y" "z" ]
        ))
    }
    scenario "Compare JSON contents (in sequence)" {
        assert(
           { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
        )
    }
    scenario "Compare JSON contents (out of sequence)" {
        assert(
           { scores: [82 78 99], id: "A1537" } 
                       matches 
           { id: "A1537", scores: [82 78 99] }
        )
    }
}
test</pre>
<h5>results</h5>
<pre>
|------------------------------------------------------------------------------------------------------------------------------|
| id | seq | level | item                                                                                    | passed | failed |
|------------------------------------------------------------------------------------------------------------------------------|
| 0  | 0   | 0     | Matches function                                                                        | 4      | 0      |
| 1  | 1   | 1     | Compare Array contents: Equal                                                           | 1      | 0      |
| 2  | 2   | 2     | assert [1, "a", "b", "c"] matches [1, "a", "b", "c"]                                    | 1      | 0      |
| 3  | 3   | 1     | Compare Array contents: Not Equal                                                       | 1      | 0      |
| 4  | 4   | 2     | assert !([1, "a", "b", "c"] matches [0, "x", "y", "z"])                                 | 1      | 0      |
| 5  | 5   | 1     | Compare JSON contents (in sequence)                                                     | 1      | 0      |
| 6  | 6   | 2     | assert {first: "Tom", last: "Lane"} matches {first: "Tom", last: "Lane"}                | 1      | 0      |
| 7  | 7   | 1     | Compare JSON contents (out of sequence)                                                 | 1      | 0      |
| 8  | 8   | 2     | assert {scores: [82, 78, 99], id: "A1537"} matches {id: "A1537", scores: [82, 78, 99]}  | 1      | 0      |
|------------------------------------------------------------------------------------------------------------------------------|

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
<hr>
<h4>▶️ Zip</h4>
<h5>example¹</h5>
<pre>[1, 2, 3] <|> ['A','B','C']</pre>
<h5>results</h5>
<pre>
[(1, 'A'), (2, 'B'), (3, 'C')]

</pre>
