📦 Platform Examples
========================================
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
27
</pre>
<hr>
<h4>📦 cal::hour12 &#8212; Returns the hour of the day of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::hour12()</pre>
<h5>results</h5>
<pre>
1
</pre>
<hr>
<h4>📦 cal::hour24 &#8212; Returns the hour (military time) of the day of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::hour24()</pre>
<h5>results</h5>
<pre>
13
</pre>
<hr>
<h4>📦 cal::minute_of &#8212; Returns the minute of the hour of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::minute_of()</pre>
<h5>results</h5>
<pre>
41
</pre>
<hr>
<h4>📦 cal::month_of &#8212; Returns the month of the year of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::month_of()</pre>
<h5>results</h5>
<pre>
5
</pre>
<hr>
<h4>📦 cal::second_of &#8212; Returns the seconds of the minute of a Date</h4>
<h5>example1</h5>
<pre>use cal
now():::second_of()</pre>
<h5>results</h5>
<pre>
10
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
2025-05-27T20:41:10.889Z
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
<h4>📦 nsd::exists &#8212; Returns true if the source path exists</h4>
<h5>example1</h5>
<pre>nsd::save('packages.exists.stocks', new Table(
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
<h4>📦 nsd::load &#8212; Loads a dataframe from a namespace</h4>
<h5>example1</h5>
<pre>let stocks =
   nsd::save('packages.save_load.stocks', new Table(
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
<h4>📦 nsd::remove &#8212; Deletes a dataframe from a namespace</h4>
<h5>example1</h5>
<pre>nsd::save('packages.remove.stocks', new Table(
    symbol: String(8),
    exchange: String(8),
    last_sale: f64
))

nsd::remove('packages.remove.stocks')
nsd::exists('packages.remove.stocks')</pre>
<h5>results</h5>
<pre>
false
</pre>
<hr>
<h4>📦 nsd::save &#8212; Creates a new dataframe</h4>
<h5>example1</h5>
<pre>let stocks =
   nsd::save('packages.save.stocks', new Table(
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
<pre>create table ns("examples.os.call") (
    symbol: String(8),
    exchange: String(8),
    last_sale: f64
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
prefix = iff(cur_dir:::ends_with("core"), "../..", ".")
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
| 25 | LaunchInstanceID           | BA12FBA4-2DD3-4D24-91D2-8D038ED4B2F9                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 26 | OLDPWD                     | /                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 27 | OXIDE_HOME                 | /Users/ldaniels/GitHub/oxide/oxide_db                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 28 | PATH                       | /Users/ldaniels/.bun/bin:/Users/ldaniels/.sdkman/candidates/java/current/bin:/usr/local/bin:/System/Cryptexes/App/usr/bin:/usr/bin:/bin:/usr/sbin:/sbin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/local/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/appleinternal/bin:/Users/ldaniels/.cargo/bin:/opt/homebrew/bin:.                                                                                                                                  |
| 29 | PWD                        | /Users/ldaniels/GitHub/oxide                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 30 | RR_REAL_RUSTDOC            | /Users/ldaniels/.cargo/bin/rustdoc                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 31 | RUSTC                      | /Users/ldaniels/.cargo/bin/rustc                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 32 | RUSTC_BOOTSTRAP            | 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 33 | RUSTDOC                    | /Users/ldaniels/Library/Application Support/JetBrains/IntelliJIdea2025.1/plugins/intellij-rust/bin/mac/aarch64/intellij-rust-native-helper                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 34 | RUSTUP_HOME                | /Users/ldaniels/.rustup                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 35 | RUSTUP_TOOLCHAIN           | stable-aarch64-apple-darwin                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 36 | RUST_BACKTRACE             | short                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 37 | RUST_RECURSION_COUNT       | 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 38 | SDKMAN_CANDIDATES_API      | https://api.sdkman.io/2                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 39 | SDKMAN_CANDIDATES_DIR      | /Users/ldaniels/.sdkman/candidates                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 40 | SDKMAN_DIR                 | /Users/ldaniels/.sdkman                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 41 | SDKMAN_PLATFORM            | darwinarm64                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 42 | SECURITYSESSIONID          | 186b1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 43 | SHELL                      | /bin/zsh                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 44 | SSH_AUTH_SOCK              | /private/tmp/com.apple.launchd.ueG9OnvHQk/Listeners                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 45 | TERM                       | ansi                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 46 | TMPDIR                     | /var/folders/ld/hwrvzn011w79gftyb6vj8mg40000gn/T/                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 47 | USER                       | ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 48 | XPC_FLAGS                  | 0x0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 49 | XPC_SERVICE_NAME           | application.com.jetbrains.intellij.505803.104316344                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 50 | __CFBundleIdentifier       | com.jetbrains.intellij                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 51 | __CF_USER_TEXT_ENCODING    | 0x1F5:0x0:0x0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
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
|-------------------------------------------------------------------------------|
| id | name    | module | signature        | description              | returns |
|-------------------------------------------------------------------------------|
| 0  | to_u128 | util   | util::to_u128(a) | Converts a value to u128 | u128    |
| 1  | to_i64  | util   | util::to_i64(a)  | Converts a value to i64  | i64     |
| 2  | to_i128 | util   | util::to_i128(a) | Converts a value to i128 | i128    |
|-------------------------------------------------------------------------------|
</pre>
<hr>
<h4>📦 oxide::home &#8212; Returns the Oxide home directory path</h4>
<h5>example1</h5>
<pre>oxide::home()</pre>
<h5>results</h5>
<pre>
"/Users/ldaniels/GitHub/oxide/oxide_db"
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
<h4>📦 oxide::uuid &#8212; Returns a random 128-bit UUID</h4>
<h5>example1</h5>
<pre>oxide::uuid()</pre>
<h5>results</h5>
<pre>
3330963f-e817-4c21-8aa9-eb719c4d051f
</pre>
<hr>
<h4>📦 oxide::version &#8212; Returns the Oxide version</h4>
<h5>example1</h5>
<pre>oxide::version()</pre>
<h5>results</h5>
<pre>
"0.40"
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
<h4>📦 testing::assert &#8212; Evaluates an assertion returning true or an error</h4>
<h5>example1</h5>
<pre>use testing
assert(
   [ 1 "a" "b" "c" ] matches [ 1 "a" "b" "c" ]
)</pre>
<h5>results</h5>
<pre>
true
</pre>
<hr>
<h4>📦 testing::feature &#8212; Creates a new test feature</h4>
<h5>example1</h5>
<pre>use testing
feature("Matches function", {
    "Compare Array contents: Equal": ctx -> {
        assert(
            [ 1 "a" "b" "c" ] matches [ 1 "a" "b" "c" ]
        )
    },
    "Compare Array contents: Not Equal": ctx -> {
        assert(!(
            [ 1 "a" "b" "c" ] matches [ 0 "x" "y" "z" ]
        ))
    },
    "Compare JSON contents (in sequence)": ctx -> {
        assert(
            { first: "Tom" last: "Lane" } matches { first: "Tom" last: "Lane" }
        )
    },
    "Compare JSON contents (out of sequence)": ctx -> {
        assert(
            { scores: [82 78 99], id: "A1537" }
                       matches 
            { id: "A1537", scores: [82 78 99] }
        )
    }
})</pre>
<h5>results</h5>
<pre>
|------------------------------------------------------------------------------------------------------------------------|
| id | level | item                                                                                    | passed | result |
|------------------------------------------------------------------------------------------------------------------------|
| 0  | 0     | Matches function                                                                        | true   | true   |
| 1  | 1     | Compare Array contents: Equal                                                           | true   | true   |
| 2  | 2     | assert([1, "a", "b", "c"] matches [1, "a", "b", "c"])                                   | true   | true   |
| 3  | 1     | Compare Array contents: Not Equal                                                       | true   | true   |
| 4  | 2     | assert(!([1, "a", "b", "c"] matches [0, "x", "y", "z"]))                                | true   | true   |
| 5  | 1     | Compare JSON contents (in sequence)                                                     | true   | true   |
| 6  | 2     | assert({first: "Tom", last: "Lane"} matches {first: "Tom", last: "Lane"})               | true   | true   |
| 7  | 1     | Compare JSON contents (out of sequence)                                                 | true   | true   |
| 8  | 2     | assert({scores: [82, 78, 99], id: "A1537"} matches {id: "A1537", scores: [82, 78, 99]}) | true   | true   |
|------------------------------------------------------------------------------------------------------------------------|
</pre>
<hr>
<h4>📦 testing::type_of &#8212; Returns the type of a value</h4>
<h5>example1</h5>
<pre>testing::type_of([12, 76, 444])</pre>
<h5>results</h5>
<pre>
"Array(3)"
</pre>
<hr>
<h4>📦 tools::compact &#8212; Shrinks a table by removing deleted rows</h4>
<h5>example1</h5>
<pre>stocks = ns("examples.compact.stocks")
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
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
<h4>📦 tools::fetch &#8212; Retrieves a raw structure from a table</h4>
<h5>example1</h5>
<pre>stocks = ns("examples.fetch.stocks")
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
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
<h4>📦 tools::journal &#8212; Retrieves the journal for an event-source or table function</h4>
<h5>example1</h5>
<pre>use tools
stocks = ns("examples.journal.stocks")
drop table stocks
create table stocks fn(
   symbol: String(8), exchange: String(8), last_sale: f64
) => {
    symbol: symbol,
    exchange: exchange,
    last_sale: last_sale,
    ingest_time: cal::now()
}
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
<h4>📦 tools::len &#8212; Returns the length of a table</h4>
<h5>example1</h5>
<pre>stocks = ns("examples.table_len.stocks")
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
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
<pre>stocks = ns("examples.map_over_table.stocks")
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[{ symbol: "WKRP", exchange: "NYSE", last_sale: 11.11 },
 { symbol: "ACDC", exchange: "AMEX", last_sale: 35.11 },
 { symbol: "UELO", exchange: "NYSE", last_sale: 90.12 }] ~> stocks
use tools
stocks:::map(fn(row) => {
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
| 0  | WKRP   | NYSE     | 11.11     | 2025-05-27T20:41:11.265Z |
| 1  | ACDC   | AMEX     | 35.11     | 2025-05-27T20:41:11.266Z |
| 2  | UELO   | NYSE     | 90.12     | 2025-05-27T20:41:11.266Z |
|---------------------------------------------------------------|
</pre>
<hr>
<h4>📦 tools::pop &#8212; Removes and returns a value or object from a Sequence</h4>
<h5>example1</h5>
<pre>use tools
stocks = ns("examples.tools_pop.stocks")
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
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
stocks = ns("examples.push.stocks")
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
stocks:::push({ symbol: "XYZ", exchange: "NASDAQ", last_sale: 24.78 })
stocks</pre>
<h5>results</h5>
<pre>
|-------------------------------|
| symbol | exchange | last_sale |
|-------------------------------|
| ABC    | AMEX     | 12.49     |
| BOOM   | NYSE     | 56.88     |
| JET    | NASDAQ   | 32.12     |
| XYZ    | NASDAQ   | 24.78     |
|-------------------------------|
</pre>
<hr>
<h4>📦 tools::replay &#8212; Reconstructs the state of a journaled table</h4>
<h5>example1</h5>
<pre>use tools
stocks = ns("examples.table_fn.stocks")
drop table stocks
create table stocks fn(
   symbol: String(8), exchange: String(8), last_sale: f64
) => {
    symbol: symbol,
    exchange: exchange,
    last_sale: last_sale * 2.0,
    rank: __row_id__ + 1
}
[{ symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
stocks:::replay()</pre>
<h5>results</h5>
<pre>
3
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
stocks = ns("examples.scan.stocks")
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
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
stocks = ns("examples.csv.stocks")
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
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
stocks = ns("examples.json.stocks")
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
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
<pre>www::serve(8822)
stocks = ns("examples.www.quotes")
table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 },
 { symbol: "BOX", exchange: "NYSE", last_sale: 56.88 },
 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
 { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
 { symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }] ~> stocks
GET http://localhost:8822/examples/www/quotes/1/4</pre>
<h5>results</h5>
<pre>
[{"exchange":"NYSE","last_sale":56.88,"symbol":"BOX"}, {"exchange":"NASDAQ","last_sale":32.12,"symbol":"JET"}, {"exchange":"AMEX","last_sale":12.49,"symbol":"ABC"}]
</pre>
