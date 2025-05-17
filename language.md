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
<pre>// Arrays can be transformed via the 'arrays' package

arrays::reverse([1, 4, 2, 8, 5, 7])</pre>
<h5>results</h5>
<pre>
[7, 5, 8, 2, 4, 1]
</pre>
<hr>
<h4>▶️ Arrays: Indexing</h4>
<h5>example¹</h5>
<pre>arr := [1, 4, 2, 8, 5, 7]
arr[3]</pre>
<h5>results</h5>
<pre>
8
</pre>
<hr>
<h4>▶️ Assignment</h4>
<h5>example¹</h5>
<pre>a := 3
b := 5
c := 7
a + b + c</pre>
<h5>results</h5>
<pre>
15
</pre>
<h5>example²</h5>
<pre>(a, b, c) := (3, 5, 7)
a + b + c</pre>
<h5>results</h5>
<pre>
15
</pre>
<h5>example³</h5>
<pre>[a, b, c] := [3, 5, 7]
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
<h4>▶️ Code Block</h4>
<h5>example¹</h5>
<pre>result := {
    (a, b, sum) := (0, 1, 0)
    while sum < 10 {
        sum := sum + (a + b)
        t := b
        b := a + b
        a := t
    }
    sum
}
result</pre>
<h5>results</h5>
<pre>
11
</pre>
<hr>
<h4>▶️ Conditionals</h4>
<h5>example¹</h5>
<pre>x := 10
x between 5 and 10</pre>
<h5>results</h5>
<pre>
true
</pre>
<h5>example²</h5>
<pre>x := 10
x betwixt 5 and 10</pre>
<h5>results</h5>
<pre>
false
</pre>
<h5>example³</h5>
<pre>x := 1..8
x contains 7</pre>
<h5>results</h5>
<pre>
7
</pre>
<hr>
<h4>▶️ Curvy-Arrow Left</h4>
<h5>example¹</h5>
<pre>stocks := ns("expressions.read_next_row.stocks")
table(symbol: String(8), exchange: String(8), history: Table(last_sale: f64, processed_time: Date)) ~> stocks
rows := [{ symbol: "BIZ", exchange: "NYSE" }, { symbol: "GOTO", exchange: "OTC" }]
rows ~> stocks
// read the last row
last_row <~ stocks
last_row</pre>
<h5>results</h5>
<pre>
{"exchange":"OTC","history":null,"symbol":"GOTO"}
</pre>
<hr>
<h4>▶️ Curvy-Arrow Right</h4>
<h5>example¹</h5>
<pre>stocks := ns("expressions.into.stocks")
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
rows := [
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
<h4>▶️ Functions</h4>
<h5>example¹</h5>
<pre>product := fn (a, b) => a * b
product(2, 5)</pre>
<h5>results</h5>
<pre>
10
</pre>
<hr>
<h4>▶️ HTTP</h4>
<h5>example¹</h5>
<pre>stocks := ns("readme.www.stocks")
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
www::serve(8833)</pre>
<h5>results</h5>
<pre>
true
</pre>
<h5>example²</h5>
<pre>POST {
    url: http://localhost:8833/platform/www/stocks/0
    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.77 }
}</pre>
<h5>results</h5>
<pre>
45
</pre>
<h5>example³</h5>
<pre>GET http://localhost:8833/platform/www/stocks/0</pre>
<h5>results</h5>
<pre>
{}
</pre>
<h5>example⁴</h5>
<pre>HEAD http://localhost:8833/platform/www/stocks/0</pre>
<h5>results</h5>
<pre>
{content-length: "81", content-type: "application/json", date: "Sat, 17 May 2025 17:12:54 GMT"}
</pre>
<h5>example⁵</h5>
<pre>PUT {
    url: http://localhost:8833/platform/www/stocks/0
    body: { symbol: "ABC", exchange: "AMEX", last_sale: 11.79 }
}</pre>
<h5>results</h5>
<pre>
1
</pre>
<h5>example⁶</h5>
<pre>GET http://localhost:8833/platform/www/stocks/0</pre>
<h5>results</h5>
<pre>
{exchange: "AMEX", last_sale: 11.79, symbol: "ABC"}
</pre>
<h5>example⁷</h5>
<pre>PATCH {
    url: http://localhost:8833/platform/www/stocks/0
    body: { last_sale: 11.81 }
}</pre>
<h5>results</h5>
<pre>
1
</pre>
<h5>example⁸</h5>
<pre>GET http://localhost:8833/platform/www/stocks/0</pre>
<h5>results</h5>
<pre>
{exchange: "AMEX", last_sale: 11.81, symbol: "ABC"}
</pre>
<h5>example⁹</h5>
<pre>DELETE http://localhost:8833/platform/www/stocks/0</pre>
<h5>results</h5>
<pre>
1
</pre>
<h5>example¹⁰</h5>
<pre>GET http://localhost:8833/platform/www/stocks/0</pre>
<h5>results</h5>
<pre>
{}
</pre>
<hr>
<h4>▶️ Implicit Method Call</h4>
<h5>example¹</h5>
<pre>import durations
8:::hours()</pre>
<h5>results</h5>
<pre>
28800000
</pre>
<hr>
<h4>▶️ Imports</h4>
<h5>example¹</h5>
<pre>import tools
stocks := to_table([
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
<h4>▶️ Iteration</h4>
<h5>example¹</h5>
<pre>foreach row in tools::to_table(['apple', 'berry', 'kiwi', 'lime']) {
    oxide::println(row)
}</pre>
<h5>results</h5>
<pre>
true
</pre>
<hr>
<h4>▶️ Mathematics: addition</h4>
<h5>example¹</h5>
<pre>a := (2, 4, 6)
b := (1, 2, 3)
a + b</pre>
<h5>results</h5>
<pre>
(3, 6, 9)
</pre>
<hr>
<h4>▶️ Mathematics: division</h4>
<h5>example¹</h5>
<pre>20.0 / 3</pre>
<h5>results</h5>
<pre>
6.666666666666667
</pre>
<h5>example²</h5>
<pre>a := (3.0, 5.0, 9.0)
b := (1.0, 2.0, 1.0)
a / b</pre>
<h5>results</h5>
<pre>
(3, 2.5, 9)
</pre>
<hr>
<h4>▶️ Mathematics: multiplication</h4>
<h5>example¹</h5>
<pre>a := (3, 5, 7)
b := (1, 0, 1)
a * b</pre>
<h5>results</h5>
<pre>
(3, 0, 7)
</pre>
<hr>
<h4>▶️ Mathematics: subtraction</h4>
<h5>example¹</h5>
<pre>188 - 36</pre>
<h5>results</h5>
<pre>
152
</pre>
<h5>example²</h5>
<pre>a := (3, 5, 7)
b := (1, 0, 1)
a - b</pre>
<h5>results</h5>
<pre>
(2, 5, 6)
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
<pre>i := 75
j := -i
j</pre>
<h5>results</h5>
<pre>
-75
</pre>
<hr>
<h4>▶️ New Instances</h4>
<h5>example¹</h5>
<pre>new Table(symbol: String(8), exchange: String(8), last_sale: f64)</pre>
<h5>results</h5>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
|------------------------------------|
</pre>
<hr>
<h4>▶️ Query</h4>
<h5>example¹</h5>
<pre>stocks := tools::to_table([
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
<pre>range := 1..5
tools::reverse(range)</pre>
<h5>results</h5>
<pre>
[4, 3, 2, 1]
</pre>
<hr>
<h4>▶️ Testing</h4>
<h5>example¹</h5>
<pre>import testing
Feature "Matches function" {
    Scenario "Compare Array contents: Equal" {
        assert(matches(
            [ 1 "a" "b" "c" ],
            [ 1 "a" "b" "c" ]
        ))
    }
    Scenario "Compare Array contents: Not Equal" {
        assert(!matches(
            [ 1 "a" "b" "c" ],
            [ 0 "x" "y" "z" ]
        ))
    }
    Scenario "Compare JSON contents (in sequence)" {
        assert(matches(
                { first: "Tom" last: "Lane" },
                { first: "Tom" last: "Lane" }))
    }
    Scenario "Compare JSON contents (out of sequence)" {
        assert(matches(
                { scores: [82 78 99], id: "A1537" },
                { id: "A1537", scores: [82 78 99] }))
    }
}</pre>
<h5>results</h5>
<pre>
|--------------------------------------------------------------------------------------------------------------------------|
| id | level | item                                                                                      | passed | result |
|--------------------------------------------------------------------------------------------------------------------------|
| 0  | 0     | Matches function                                                                          | true   | true   |
| 1  | 1     | Compare Array contents: Equal                                                             | true   | true   |
| 2  | 2     | assert(matches([1, "a", "b", "c"], [1, "a", "b", "c"]))                                   | true   | true   |
| 3  | 1     | Compare Array contents: Not Equal                                                         | true   | true   |
| 4  | 2     | assert(!matches([1, "a", "b", "c"], [0, "x", "y", "z"]))                                  | true   | true   |
| 5  | 1     | Compare JSON contents (in sequence)                                                       | true   | true   |
| 6  | 2     | assert(matches({first: "Tom", last: "Lane"}, {first: "Tom", last: "Lane"}))               | true   | true   |
| 7  | 1     | Compare JSON contents (out of sequence)                                                   | true   | true   |
| 8  | 2     | assert(matches({scores: [82, 78, 99], id: "A1537"}, {id: "A1537", scores: [82, 78, 99]})) | true   | true   |
|--------------------------------------------------------------------------------------------------------------------------|
</pre>
<hr>
<h4>▶️ Type Definitions</h4>
<h5>example¹</h5>
<pre>LabelString := typedef(String(80))
LabelString</pre>
<h5>results</h5>
<pre>
String(80)
</pre>
<hr>
<h4>▶️ Via Clause</h4>
<h5>example¹</h5>
<pre>stocks := ns("readme.via.stocks")
drop table stocks
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks

rows := [
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
<h4>▶️ if / iff</h4>
<h5>example¹</h5>
<pre>x := 4
if(x > 5) "Yes"
else if(x < 5) "Maybe"
else "No"</pre>
<h5>results</h5>
<pre>
"Maybe"
</pre>
<h5>example²</h5>
<pre>fact := fn(n) => iff(n <= 1, 1, n * fact(n - 1))
fact(6)</pre>
<h5>results</h5>
<pre>
720
</pre>
