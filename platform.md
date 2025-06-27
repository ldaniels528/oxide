📦 Platform Examples
========================================
<hr>
<h4>📦 agg::avg &#8212; returns the average of values in a column</h4>
<h5>example1</h5>
<pre>use agg
select exchange, avg_sale: avg(last_sale)
from
    |--------------------------------|
    | symbol | exchange  | last_sale |
    |--------------------------------|
    | GIF    | NYSE      | 11.77     |
    | TRX    | NASDAQ    | 32.97     |
    | RLP    | NYSE      | 23.66     |
    | GTO    | NASDAQ    | 51.23     |
    | BST    | NASDAQ    | 214.88    |
    |--------------------------------|
group_by exchange</pre>
<h5>results</h5>
<pre>
|-----------------------------------|
| id | exchange | avg_sale          |
|-----------------------------------|
| 0  | NYSE     | 17.715            |
| 1  | NASDAQ   | 99.69333333333333 |
|-----------------------------------|

</pre>
<hr>
<h4>📦 agg::count &#8212; returns the counts of rows or non-null fields</h4>
<h5>example1</h5>
<pre>use agg
select exchange, qty: count(last_sale)
from
    |--------------------------------|
    | symbol | exchange  | last_sale |
    |--------------------------------|
    | GIF    | NYSE      | 11.77     |
    | TRX    | NASDAQ    | 32.97     |
    | RLP    | NYSE      | 23.66     |
    | GTO    | NASDAQ    | 51.23     |
    | BST    | NASDAQ    | 214.88    |
    |--------------------------------|
group_by exchange</pre>
<h5>results</h5>
<pre>
|---------------------|
| id | exchange | qty |
|---------------------|
| 0  | NASDAQ   | 3   |
| 1  | NYSE     | 2   |
|---------------------|

</pre>
<hr>
<h4>📦 agg::max &#8212; returns the maximum value of a collection of fields</h4>
<h5>example1</h5>
<pre>use agg
select exchange, max_sale: max(last_sale)
from
    |--------------------------------|
    | symbol | exchange  | last_sale |
    |--------------------------------|
    | GIF    | NYSE      | 11.77     |
    | TRX    | NASDAQ    | 32.97     |
    | RLP    | NYSE      | 23.66     |
    | GTO    | NASDAQ    | 51.23     |
    | BST    | NASDAQ    | 214.88    |
    |--------------------------------|
group_by exchange</pre>
<h5>results</h5>
<pre>
|--------------------------|
| id | exchange | max_sale |
|--------------------------|
| 0  | NYSE     | 23.66    |
| 1  | NASDAQ   | 214.88   |
|--------------------------|

</pre>
<hr>
<h4>📦 agg::min &#8212; returns the minimum value of a collection of fields</h4>
<h5>example1</h5>
<pre>use agg
select exchange, min_sale: min(last_sale)
from
    |--------------------------------|
    | symbol | exchange  | last_sale |
    |--------------------------------|
    | GIF    | NYSE      | 11.77     |
    | TRX    | NASDAQ    | 32.97     |
    | RLP    | NYSE      | 23.66     |
    | GTO    | NASDAQ    | 51.23     |
    | BST    | NASDAQ    | 214.88    |
    |--------------------------------|
group_by exchange</pre>
<h5>results</h5>
<pre>
|--------------------------|
| id | exchange | min_sale |
|--------------------------|
| 0  | NASDAQ   | 32.97    |
| 1  | NYSE     | 11.77    |
|--------------------------|

</pre>
<hr>
<h4>📦 agg::sum &#8212; returns the sum of a collection of fields</h4>
<h5>example1</h5>
<pre>use agg
select exchange, total_sale: sum(last_sale)
from
    |--------------------------------|
    | symbol | exchange  | last_sale |
    |--------------------------------|
    | GIF    | NYSE      | 11.77     |
    | TRX    | NASDAQ    | 32.97     |
    | RLP    | NYSE      | 23.66     |
    | GTO    | NASDAQ    | 51.23     |
    | BST    | NASDAQ    | 214.88    |
    |--------------------------------|
group_by exchange</pre>
<h5>results</h5>
<pre>
|----------------------------|
| id | exchange | total_sale |
|----------------------------|
| 0  | NYSE     | 35.43      |
| 1  | NASDAQ   | 299.08     |
|----------------------------|

</pre>
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
tools::to_table(stocks)</pre>
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
27

</pre>
<hr>
<h4>📦 cal::hour12 &#8212; Returns the hour of the day of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::hour12()</pre>
<h5>results</h5>
<pre>
11

</pre>
<hr>
<h4>📦 cal::hour24 &#8212; Returns the hour (military time) of the day of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::hour24()</pre>
<h5>results</h5>
<pre>
11

</pre>
<hr>
<h4>📦 cal::minute_of &#8212; Returns the minute of the hour of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::minute_of()</pre>
<h5>results</h5>
<pre>
15

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
25

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
2025-06-27T18:15:25.505Z

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
0v47338bd5f35bbb239092c36e30775b4a

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
<h4>📦 nsd::resize &#8212; Changes the size of a dataframe</h4>
<h5>example1</h5>
<pre>use nsd
let stocks =
   nsd::save('packages.resize.stocks', Table::new(
       symbol: String(8),
       exchange: String(8),
       last_sale: f64
   ))
[{ symbol: "TCO", exchange: "NYSE", last_sale: 38.53 },
 { symbol: "SHMN", exchange: "NYSE", last_sale: 6.57 },
 { symbol: "HMU", exchange: "NASDAQ", last_sale: 27.12 }] ~> stocks
'packages.resize.stocks':::resize(1)
stocks</pre>
<h5>results</h5>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 0  | TCO    | NYSE     | 38.53     |
|------------------------------------|

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
[{ symbol: "TCO", exchange: "NYSE", last_sale: 38.53 },
 { symbol: "SHMN", exchange: "NYSE", last_sale: 6.57 },
 { symbol: "HMU", exchange: "NASDAQ", last_sale: 27.12 }] ~> stocks
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
| 20 | DYLD_FALLBACK_LIBRARY_PATH | /Users/ldaniels/GitHub/oxide/target/debug/build/curl-sys-60007e9a24d74562/out/build:/Users/ldaniels/GitHub/oxide/target/debug/build/libnghttp2-sys-03d0e22189823925/out/i/lib:/Users/ldaniels/GitHub/oxide/target/debug/build/zstd-sys-b2743e594d963e4d/out:/Users/ldaniels/GitHub/oxide/target/debug/deps:/Users/ldaniels/GitHub/oxide/target/debug:/Users/ldaniels/.rustup/toolchains/stable-aarch64-apple-darwin/lib/rustlib/aarch64-apple-darwin/lib:/Users/ldaniels/.rustup/toolchains/stable-aarch64-apple-darwin/lib:/Users/ldaniels/lib:/usr/local/lib:/usr/lib |
| 21 | HOME                       | /Users/ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 22 | IJ_RESTARTER_LOG           | /Users/ldaniels/Library/Logs/JetBrains/IntelliJIdea2025.1/restarter.log                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 23 | JAVA_HOME                  | /Users/ldaniels/.sdkman/candidates/java/current                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| 24 | LC_CTYPE                   | en_US.UTF-8                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 25 | LOGNAME                    | ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 26 | OLDPWD                     | /                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 27 | PATH                       | /Users/ldaniels/.bun/bin:/Users/ldaniels/.sdkman/candidates/java/current/bin:/usr/local/bin:/System/Cryptexes/App/usr/bin:/usr/bin:/bin:/usr/sbin:/sbin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/local/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/appleinternal/bin:/Users/ldaniels/.cargo/bin:/opt/homebrew/bin:.                                                                                                                                  |
| 28 | PWD                        | /Users/ldaniels/GitHub/oxide                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 29 | RR_REAL_RUSTDOC            | /Users/ldaniels/.cargo/bin/rustdoc                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 30 | RUSTC                      | /Users/ldaniels/.cargo/bin/rustc                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 31 | RUSTC_BOOTSTRAP            | 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 32 | RUSTDOC                    | /Users/ldaniels/Library/Application Support/JetBrains/IntelliJIdea2025.1/plugins/intellij-rust/bin/mac/aarch64/intellij-rust-native-helper                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 33 | RUSTUP_HOME                | /Users/ldaniels/.rustup                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 34 | RUSTUP_TOOLCHAIN           | stable-aarch64-apple-darwin                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 35 | RUST_BACKTRACE             | short                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 36 | RUST_RECURSION_COUNT       | 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 37 | SDKMAN_CANDIDATES_API      | https://api.sdkman.io/2                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 38 | SDKMAN_CANDIDATES_DIR      | /Users/ldaniels/.sdkman/candidates                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 39 | SDKMAN_DIR                 | /Users/ldaniels/.sdkman                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 40 | SDKMAN_PLATFORM            | darwinarm64                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 41 | SHELL                      | /bin/zsh                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 42 | SSH_AUTH_SOCK              | /private/tmp/com.apple.launchd.lSmiPPAhaN/Listeners                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 43 | TERM                       | ansi                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 44 | TMPDIR                     | /var/folders/ld/hwrvzn011w79gftyb6vj8mg40000gn/T/                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 45 | USER                       | ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 46 | XPC_FLAGS                  | 0x0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 47 | XPC_SERVICE_NAME           | application.com.jetbrains.intellij.505803.114443456.D4DEF3B6-6757-4DBB-BE00-518FF51FABEF                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 48 | __CFBundleIdentifier       | com.jetbrains.intellij                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 49 | __CF_USER_TEXT_ENCODING    | 0x1F5:0x0:0x0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
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
Ok(Pow(Literal(Number(I64Value(2))), Literal(Number(I64Value(4)))))

</pre>
<hr>
<h4>📦 oxide::eval &#8212; Evaluates a string containing Oxide code</h4>
<h5>example1</h5>
<pre>a = 'Hello '
b = 'World'
oxide::eval("a + b")</pre>
<h5>results</h5>
<pre>
Hello World

</pre>
<hr>
<h4>📦 oxide::help &#8212; Integrated help function</h4>
<h5>example1</h5>
<pre>oxide::help() limit 3</pre>
<h5>results</h5>
<pre>
|----------------------------------------------------------------------------------------------------------|
| id | name       | module | signature                  | description                            | returns |
|----------------------------------------------------------------------------------------------------------|
| 0  | url_encode | www    | www::url_encode(s: String) | Encodes a URL string                   | String  |
| 1  | url_decode | www    | www::url_decode(s: String) | Decodes a URL-encoded string           | String  |
| 2  | sqrt       | math   | math::sqrt(n: f64)         | sqrt(x): Returns the square root of x. | i64     |
|----------------------------------------------------------------------------------------------------------|

</pre>
<hr>
<h4>📦 oxide::home &#8212; Returns the Oxide home directory path</h4>
<h5>example1</h5>
<pre>oxide::home()</pre>
<h5>results</h5>
<pre>
/Users/ldaniels/oxide_db

</pre>
<hr>
<h4>📦 oxide::inspect &#8212; Returns a table describing the structure of a model</h4>
<h5>example1</h5>
<pre>oxide::inspect("{ x = 1 x = x + 1 }")</pre>
<h5>results</h5>
<pre>
|-----------------------------------------------------------------------------------------------------|
| id | code      | model                                                                              |
|-----------------------------------------------------------------------------------------------------|
| 0  | x = 1     | SetVariables(Identifier("x"), Literal(Number(I64Value(1))))                        |
| 1  | x = x + 1 | SetVariables(Identifier("x"), Plus(Identifier("x"), Literal(Number(I64Value(1))))) |
|-----------------------------------------------------------------------------------------------------|

</pre>
<hr>
<h4>📦 oxide::inspect &#8212; Returns a table describing the structure of a model</h4>
<h5>example2</h5>
<pre>oxide::inspect("stock::is_this_you('ABC')")</pre>
<h5>results</h5>
<pre>
|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| id | code                      | model                                                                                                                |
|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| 0  | stock::is_this_you("ABC") | ColonColon(Identifier("stock"), FunctionCall { fx: Identifier("is_this_you"), args: [Literal(StringValue("ABC"))] }) |
|-------------------------------------------------------------------------------------------------------------------------------------------------------|

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
Hello World

</pre>
<hr>
<h4>📦 oxide::uuid &#8212; Returns a random 128-bit UUID</h4>
<h5>example1</h5>
<pre>oxide::uuid()</pre>
<h5>results</h5>
<pre>
de6eb23c-c233-44b8-8f58-42c7ccdc5cc3

</pre>
<hr>
<h4>📦 oxide::version &#8212; Returns the Oxide version</h4>
<h5>example1</h5>
<pre>oxide::version()</pre>
<h5>results</h5>
<pre>
0.46

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
This is the way

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
1, 5, 9, 13

</pre>
<hr>
<h4>📦 str::left &#8212; Returns n-characters from left-to-right</h4>
<h5>example1</h5>
<pre>str::left('Hello World', 5)</pre>
<h5>results</h5>
<pre>
Hello

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
World

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
|stocks
|where exchange is 'NYSE'
", '|')</pre>
<h5>results</h5>
<pre>

Code example:

stocks
where exchange is 'NYSE'

</pre>
<hr>
<h4>📦 str::substring &#8212; Returns a substring of string `s` from `m` to `n`</h4>
<h5>example1</h5>
<pre>str::substring('Hello World', 0, 5)</pre>
<h5>results</h5>
<pre>
Hello

</pre>
<hr>
<h4>📦 str::superscript &#8212; Returns a superscript of a number `n`</h4>
<h5>example1</h5>
<pre>str::superscript(5)</pre>
<h5>results</h5>
<pre>
⁵

</pre>
<hr>
<h4>📦 str::to_string &#8212; Converts a value to its text-based representation</h4>
<h5>example1</h5>
<pre>str::to_string(125.75)</pre>
<h5>results</h5>
<pre>
125.75

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
delete stocks where last_sale > 1.0
stocks</pre>
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
<h4>📦 tools::latest &#8212; Returns the row_id of last inserted record</h4>
<h5>example1</h5>
<pre>stocks = nsd::save(
   "packages.tools_latest.stocks",
    |--------------------------------|
    | symbol | exchange  | last_sale |
    |--------------------------------|
    | GIF    | NYSE      | 11.75     |
    | TRX    | NASDAQ    | 32.96     |
    | SHMN   | OTCBB     | 5.02      |
    | XCD    | OTCBB     | 1.37      |
    | DRMQ   | OTHER_OTC | 0.02      |
    | JTRQ   | OTHER_OTC | 0.0001    |
    |--------------------------------|
)
delete stocks where last_sale < 1
row_id = tools::latest(stocks)
stocks[row_id]</pre>
<h5>results</h5>

```json
{
  "exchange": "OTCBB",
  "last_sale": 1.37,
  "symbol": "XCD"
}
```
            
<hr>
<h4>📦 tools::len &#8212; Returns the length of a table</h4>
<h5>example1</h5>
<pre>stocks = nsd::save(
   "examples.table_len.stocks",
   [{ symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
    { symbol: "ACDC", exchange: "AMEX", last_sale: 35.11 },
    { symbol: "UELO", exchange: "NYSE", last_sale: 90.12 }] 
)
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
| 0  | WKRP   | NYSE     | 11.11     | 2025-06-27T18:15:28.610Z |
| 1  | ACDC   | AMEX     | 35.11     | 2025-06-27T18:15:28.611Z |
| 2  | UELO   | NYSE     | 90.12     | 2025-06-27T18:15:28.611Z |
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
delete stocks where last_sale > 1.0
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
SGVsbG8gV29ybGQ=

</pre>
<hr>
<h4>📦 util::to_binary &#8212; Translates a numeric value into binary</h4>
<h5>example1</h5>
<pre>util::to_binary(0b1011 & 0b1101)</pre>
<h5>results</h5>
<pre>
1001

</pre>
<hr>
<h4>📦 util::gzip &#8212; Compresses bytes via gzip</h4>
<h5>example1</h5>
<pre>util::gzip('Hello World')</pre>
<h5>results</h5>
<pre>
0v1f8b08000000000000fff348cdc9c95708cf2fca49010056b1174a0b000000

</pre>
<hr>
<h4>📦 util::gunzip &#8212; Decompresses bytes via gzip</h4>
<h5>example1</h5>
<pre>util::gunzip(util::gzip('Hello World'))</pre>
<h5>results</h5>
<pre>
0v48656c6c6f20576f726c64

</pre>
<hr>
<h4>📦 util::hex &#8212; Translates bytes into hexadecimal</h4>
<h5>example1</h5>
<pre>util::hex('Hello World')</pre>
<h5>results</h5>
<pre>
48656c6c6f20576f726c64

</pre>
<hr>
<h4>📦 util::md5 &#8212; Creates a MD5 digest</h4>
<h5>example1</h5>
<pre>util::md5('Hello World')</pre>
<h5>results</h5>
<pre>
0vb10a8db164e0754105b7a99be72e3fe5

</pre>
<hr>
<h4>📦 util::round &#8212; Rounds a Float to a specific number of decimal places</h4>
<h5>example1</h5>
<pre>util::round(1.42857, 2)</pre>
<h5>results</h5>
<pre>
1.43

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
±

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
http://shocktrade.com?name=the hero&t=9998

</pre>
<hr>
<h4>📦 www::url_encode &#8212; Encodes a URL string</h4>
<h5>example1</h5>
<pre>www::url_encode('http://shocktrade.com?name=the hero&t=9998')</pre>
<h5>results</h5>
<pre>
http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998

</pre>
<hr>
<h4>📦 http::serve &#8212; Starts a local HTTP service</h4>
<h5>example1</h5>
<pre>http::serve(8787)
stocks = nsd::save(
   "examples.www.stocks",
   Table::new(symbol: String(8), exchange: String(8), last_sale: f64)
)
[{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 },
 { symbol: "BOX", exchange: "NYSE", last_sale: 56.88 },
 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
 { symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }] ~> stocks
GET http://localhost:8787/examples/www/stocks/1/4</pre>
<h5>results</h5>
<pre>
[{"exchange":"NYSE","last_sale":56.88,"symbol":"BOX"}, {"exchange":"NASDAQ","last_sale":32.12,"symbol":"JET"}, {"exchange":"AMEX","last_sale":12.49,"symbol":"ABC"}]

</pre>
