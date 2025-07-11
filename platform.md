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
| 0  | NASDAQ   | 99.69333333333333 |
| 1  | NYSE     | 17.715            |
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
0B47338bd5f35bbb239092c36e30775b4a

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
   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
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
       event_time: DateTime::new()
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
<pre>nsd::save('packages.remove.stocks', Table(
    symbol: String(8),
    exchange: String(8),
    last_sale: f64
)::new)

nsd::drop('packages.remove.stocks')
nsd::exists('packages.remove.stocks')</pre>
<h5>results</h5>
<pre>
false

</pre>
<hr>
<h4>📦 nsd::exists &#8212; Returns true if the source path exists</h4>
<h5>example1</h5>
<pre>nsd::save('packages.exists.stocks', Table(
   symbol: String(8),
   exchange: String(8),
   last_sale: f64
)::new)
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
       ingest_time: DateTime::new()
   });
[{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
 { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
 { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
stocks::journal()</pre>
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
   nsd::save('packages.save_load.stocks', Table(
       symbol: String(8),
       exchange: String(8),
       last_sale: f64
   )::new)

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
stocks::replay()</pre>
<h5>results</h5>
<pre>
3

</pre>
<hr>
<h4>📦 nsd::resize &#8212; Changes the size of a dataframe</h4>
<h5>example1</h5>
<pre>use nsd
let stocks =
   nsd::save('packages.resize.stocks', Table(
       symbol: String(8),
       exchange: String(8),
       last_sale: f64
   )::new)
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
   nsd::save('packages.save.stocks', Table(
       symbol: String(8),
       exchange: String(8),
       last_sale: f64
   )::new)
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
    Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
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
<pre>cur_dir = os::current_dir()
prefix = if(cur_dir::ends_with("core"), "../..", ".")
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
| 20 | DYLD_FALLBACK_LIBRARY_PATH | /Users/ldaniels/GitHub/oxide/target/debug/build/curl-sys-cf0413c016fc1616/out/build:/Users/ldaniels/GitHub/oxide/target/debug/build/libnghttp2-sys-cb68a1bd2d42eb7a/out/i/lib:/Users/ldaniels/GitHub/oxide/target/debug/build/zstd-sys-36009a9af2c48258/out:/Users/ldaniels/GitHub/oxide/target/debug/deps:/Users/ldaniels/GitHub/oxide/target/debug:/Users/ldaniels/.rustup/toolchains/stable-aarch64-apple-darwin/lib/rustlib/aarch64-apple-darwin/lib:/Users/ldaniels/.rustup/toolchains/stable-aarch64-apple-darwin/lib:/Users/ldaniels/lib:/usr/local/lib:/usr/lib |
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
| 41 | SSH_AUTH_SOCK              | /private/tmp/com.apple.launchd.9xJlkubfiQ/Listeners                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 42 | TERM                       | ansi                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 43 | TMPDIR                     | /var/folders/ld/hwrvzn011w79gftyb6vj8mg40000gn/T/                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| 44 | USER                       | ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| 45 | XPC_FLAGS                  | 0x0                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 46 | XPC_SERVICE_NAME           | application.com.jetbrains.intellij.505803.114443456                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
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
|-------------------------------------------------------------------------------|
| id | name    | module | signature        | description              | returns |
|-------------------------------------------------------------------------------|
| 0  | to_u8   | util   | util::to_u8(a)   | Converts a value to u8   | u8      |
| 1  | to_u64  | util   | util::to_u64(a)  | Converts a value to u64  | u64     |
| 2  | to_u128 | util   | util::to_u128(a) | Converts a value to u128 | u128    |
|-------------------------------------------------------------------------------|

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
df67ef21-a266-4f2a-af26-47a866f996dd

</pre>
<hr>
<h4>📦 oxide::version &#8212; Returns the Oxide version</h4>
<h5>example1</h5>
<pre>oxide::version()</pre>
<h5>results</h5>
<pre>
0.47

</pre>
<hr>
<h4>📦 util::base62_decode &#8212; Converts a Base62 string to binary</h4>
<h5>example1</h5>
<pre>'Hello World'::base62_encode::base62_decode::to_string</pre>
<h5>results</h5>
<pre>
     Hello World

</pre>
<hr>
<h4>📦 util::base64_decode &#8212; Converts a Base64 string to binary</h4>
<h5>example1</h5>
<pre>'Hello World'::base64_encode::base64_decode::to_string</pre>
<h5>results</h5>
<pre>
Hello World

</pre>
<hr>
<h4>📦 util::base62_encode &#8212; Converts ASCII to Base62</h4>
<h5>example1</h5>
<pre>'Hello World'::base62_encode</pre>
<h5>results</h5>
<pre>
73XpUgyMwkGr29M

</pre>
<hr>
<h4>📦 util::base64_encode &#8212; Translates bytes into Base 64</h4>
<h5>example1</h5>
<pre>'Hello World'::base64_encode</pre>
<h5>results</h5>
<pre>
SGVsbG8gV29ybGQ=

</pre>
<hr>
<h4>📦 util::gzip &#8212; Compresses bytes via gzip</h4>
<h5>example1</h5>
<pre>util::gzip('Hello World')</pre>
<h5>results</h5>
<pre>
0B1f8b08000000000000fff348cdc9c95708cf2fca49010056b1174a0b000000

</pre>
<hr>
<h4>📦 util::gunzip &#8212; Decompresses bytes via gzip</h4>
<h5>example1</h5>
<pre>util::gunzip(util::gzip('Hello World'))</pre>
<h5>results</h5>
<pre>
0B48656c6c6f20576f726c64

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
0Bb10a8db164e0754105b7a99be72e3fe5

</pre>
<hr>
<h4>📦 util::random &#8212; Returns a random numeric value</h4>
<h5>example1</h5>
<pre>util::random()</pre>
<h5>results</h5>
<pre>
14102536496611032048

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
<h4>📦 util::to_ascii &#8212; Converts an integer to ASCII</h4>
<h5>example1</h5>
<pre>util::to_ascii(177)</pre>
<h5>results</h5>
<pre>
±

</pre>
<hr>
<h4>📦 util::to_bytes &#8212; Converts a value to a ByteString</h4>
<h5>example1</h5>
<pre>'The little brown fox'::to_bytes</pre>
<h5>results</h5>
<pre>
0B546865206c6974746c652062726f776e20666f78

</pre>
<hr>
<h4>📦 util::to_date &#8212; Converts a value to DateTime</h4>
<h5>example1</h5>
<pre>util::to_date(177)</pre>
<h5>results</h5>
<pre>
1970-01-01T00:00:00.177Z

</pre>
<hr>
<h4>📦 util::to_u8 &#8212; Converts a value to u8</h4>
<h5>example1</h5>
<pre>util::to_u8(257)</pre>
<h5>results</h5>
<pre>
0x01

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
<h4>📦 util::to_u64 &#8212; Converts a value to u64</h4>
<h5>example1</h5>
<pre>util::to_u64(88)</pre>
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
<h4>📦 http::serve &#8212; Starts a local HTTP service</h4>
<h5>example1</h5>
<pre>http::serve(8787)
stocks = nsd::save(
   "examples.www.stocks",
   Table(symbol: String(8), exchange: String(8), last_sale: f64)::new
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
