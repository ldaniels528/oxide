
<a name="examples"></a>
#### Platform Examples

<hr>
<h4>cal::now &#8212; Returns the current local date and time</h4>
<pre>cal::now()</pre>
<pre>
2025-05-12T19:32:44.054Z
</pre>
<hr>
<h4>cal::day_of &#8212; Returns the day of the month of a Date</h4>
<pre>import cal
now():::day_of()</pre>
<pre>
12
</pre>
<hr>
<h4>cal::hour12 &#8212; Returns the hour of the day of a Date</h4>
<pre>import cal
now():::hour12()</pre>
<pre>
12
</pre>
<hr>
<h4>cal::hour24 &#8212; Returns the hour (military time) of the day of a Date</h4>
<pre>import cal
now():::hour24()</pre>
<pre>
12
</pre>
<hr>
<h4>cal::minute_of &#8212; Returns the minute of the hour of a Date</h4>
<pre>import cal
now():::minute_of()</pre>
<pre>
32
</pre>
<hr>
<h4>cal::month_of &#8212; Returns the month of the year of a Date</h4>
<pre>import cal
now():::month_of()</pre>
<pre>
44
</pre>
<hr>
<h4>cal::second_of &#8212; Returns the seconds of the minute of a Date</h4>
<pre>import cal
now():::second_of()</pre>
<pre>
44
</pre>
<hr>
<h4>cal::year_of &#8212; Returns the year of a Date</h4>
<pre>import cal
now():::year_of()</pre>
<pre>
2025
</pre>
<hr>
<h4>durations::days &#8212; Converts a number into the equivalent number of days</h4>
<pre>import durations
3:::days()</pre>
<pre>
259200000
</pre>
<hr>
<h4>durations::hours &#8212; Converts a number into the equivalent number of hours</h4>
<pre>import durations
8:::hours()</pre>
<pre>
28800000
</pre>
<hr>
<h4>durations::millis &#8212; Converts a number into the equivalent number of millis</h4>
<pre>import durations
8:::millis()</pre>
<pre>
8
</pre>
<hr>
<h4>durations::minutes &#8212; Converts a number into the equivalent number of minutes</h4>
<pre>import durations
30:::minutes()</pre>
<pre>
1800000
</pre>
<hr>
<h4>durations::seconds &#8212; Converts a number into the equivalent number of seconds</h4>
<pre>import durations
30:::seconds()</pre>
<pre>
30000
</pre>
<hr>
<h4>io::create_file &#8212; Creates a new file</h4>
<pre>io::create_file("quote.json", {
   symbol: "TRX",
   exchange: "NYSE",
   last_sale: 45.32
})</pre>
<pre>
52
</pre>
<hr>
<h4>io::exists &#8212; Returns true if the source path exists</h4>
<pre>io::exists("quote.json")</pre>
<pre>
true
</pre>
<hr>
<h4>io::read_text_file &#8212; Reads the contents of a text file into memory</h4>
<pre>import io, util
file := "temp_secret.txt"
file:::create_file(md5("**keep**this**secret**"))
file:::read_text_file()</pre>
<pre>
47338bd5f35bbb239092c36e30775b4a
</pre>
<hr>
<h4>io::stderr &#8212; Writes a string to STDERR</h4>
<pre>io::stderr("Goodbye Cruel World")</pre>
<pre>
true
</pre>
<hr>
<h4>io::stdout &#8212; Writes a string to STDOUT</h4>
<pre>io::stdout("Hello World")</pre>
<pre>
true
</pre>
<hr>
<h4>os::call &#8212; Invokes an operating system application</h4>
<pre>create table ns("platform.os.call") (
    symbol: String(8),
    exchange: String(8),
    last_sale: f64
)
os::call("chmod", "777", oxide::home())</pre>
<pre>

</pre>
<hr>
<h4>os::clear &#8212; Clears the terminal/screen</h4>
<pre>os::clear()</pre>
<pre>
true
</pre>
<hr>
<h4>os::current_dir &#8212; Returns the current directory</h4>
<pre>import str
cur_dir := os::current_dir()
prefix := iff(cur_dir:::ends_with("core"), "../..", ".")
path_str := prefix + "/demoes/language/include_file.oxide"
include path_str</pre>
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
<pre>os::env()</pre>
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
| 20 | DYLD_FALLBACK_LIBRARY_PATH | /Users/ldaniels/GitHub/oxide/target/debug/build/zstd-sys-fc50b23a4619b9d4/out:/Users/ldaniels/GitHub/oxide/target/debug/deps:/Users/ldaniels/GitHub/oxide/target/debug:/Users/ldaniels/.rustup/toolchains/stable-aarch64-apple-darwin/lib/rustlib/aarch64-apple-darwin/lib:/Users/ldaniels/.rustup/toolchains/stable-aarch64-apple-darwin/lib:/Users/ldaniels/lib:/usr/local/lib:/usr/lib                                              |
| 21 | HOME                       | /Users/ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 22 | IDEA_INITIAL_DIRECTORY     | /                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 23 | JAVA_HOME                  | /Users/ldaniels/.sdkman/candidates/java/current                                                                                                                                                                                                                                                                                                                                                                                        |
| 24 | LC_CTYPE                   | en_US.UTF-8                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 25 | LOGNAME                    | ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 26 | OLDPWD                     | /                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 27 | OXIDE_HOME                 | /Users/ldaniels/GitHub/oxide/oxide_db                                                                                                                                                                                                                                                                                                                                                                                                  |
| 28 | PATH                       | /Users/ldaniels/.bun/bin:/Users/ldaniels/.sdkman/candidates/java/current/bin:/usr/local/bin:/System/Cryptexes/App/usr/bin:/usr/bin:/bin:/usr/sbin:/sbin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/local/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/appleinternal/bin:/Users/ldaniels/.cargo/bin:/opt/homebrew/bin:. |
| 29 | PWD                        | /Users/ldaniels/GitHub/oxide                                                                                                                                                                                                                                                                                                                                                                                                           |
| 30 | RUSTC                      | /Users/ldaniels/.cargo/bin/rustc                                                                                                                                                                                                                                                                                                                                                                                                       |
| 31 | RUSTC_BOOTSTRAP            | 1                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 32 | RUSTUP_HOME                | /Users/ldaniels/.rustup                                                                                                                                                                                                                                                                                                                                                                                                                |
| 33 | RUSTUP_TOOLCHAIN           | stable-aarch64-apple-darwin                                                                                                                                                                                                                                                                                                                                                                                                            |
| 34 | RUST_BACKTRACE             | short                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| 35 | RUST_RECURSION_COUNT       | 1                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 36 | SDKMAN_CANDIDATES_API      | https://api.sdkman.io/2                                                                                                                                                                                                                                                                                                                                                                                                                |
| 37 | SDKMAN_CANDIDATES_DIR      | /Users/ldaniels/.sdkman/candidates                                                                                                                                                                                                                                                                                                                                                                                                     |
| 38 | SDKMAN_DIR                 | /Users/ldaniels/.sdkman                                                                                                                                                                                                                                                                                                                                                                                                                |
| 39 | SDKMAN_PLATFORM            | darwinarm64                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 40 | SHELL                      | /bin/zsh                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 41 | SSH_AUTH_SOCK              | /private/tmp/com.apple.launchd.6To52j2ZMT/Listeners                                                                                                                                                                                                                                                                                                                                                                                    |
| 42 | TERM                       | ansi                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 43 | TMPDIR                     | /var/folders/ld/hwrvzn011w79gftyb6vj8mg40000gn/T/                                                                                                                                                                                                                                                                                                                                                                                      |
| 44 | USER                       | ldaniels                                                                                                                                                                                                                                                                                                                                                                                                                               |
| 45 | XPC_FLAGS                  | 0x0                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 46 | XPC_SERVICE_NAME           | application.com.jetbrains.intellij.505803.58851138                                                                                                                                                                                                                                                                                                                                                                                     |
| 47 | __CFBundleIdentifier       | com.jetbrains.intellij                                                                                                                                                                                                                                                                                                                                                                                                                 |
| 48 | __CF_USER_TEXT_ENCODING    | 0x1F5:0x0:0x0                                                                                                                                                                                                                                                                                                                                                                                                                          |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
</pre>
<hr>
<h4>oxide::compile &#8212; Compiles source code from a string input</h4>
<pre>code := oxide::compile("2 ** 4")
code()</pre>
<pre>
16
</pre>
<hr>
<h4>oxide::debug &#8212; Compiles source code from a string input; returning a debug string</h4>
<pre>oxide::debug("2 ** 4")</pre>
<pre>
Ok(Pow(Literal(Number(I64Value(2))), Literal(Number(I64Value(4)))))
</pre>
<hr>
<h4>oxide::eval &#8212; Evaluates a string containing Oxide code</h4>
<pre>a := 'Hello '
b := 'World'
oxide::eval("a + b")</pre>
<pre>
Hello World
</pre>
<hr>
<h4>oxide::help &#8212; Integrated help function</h4>
<pre>from oxide::help() limit 3</pre>
<pre>
|------------------------------------------------------------------------------------------------------------------------------------------------|
| id | name        | module | signature         | description                                     | returns                                      |
|------------------------------------------------------------------------------------------------------------------------------------------------|
| 0  | env         | os     | os::env()         | Returns a table of the OS environment variables | Table(key: String(256), value: String(8192)) |
| 1  | current_dir | os     | os::current_dir() | Returns the current directory                   | String                                       |
| 2  | clear       | os     | os::clear()       | Clears the terminal/screen                      | Boolean                                      |
|------------------------------------------------------------------------------------------------------------------------------------------------|
</pre>
<hr>
<h4>oxide::history &#8212; Returns all commands successfully executed during the session</h4>
<pre>from oxide::history() limit 3</pre>
<pre>
|-------------------------------------------------------------------|
| id | session_id    | user_id | cpu_time_ms | input                |
|-------------------------------------------------------------------|
| 0  | 1747062794617 | 501     | 0.903       | import oxide; help() |
| 1  | 1747062794920 | 501     | 1.577       | import oxide; help() |
|-------------------------------------------------------------------|
</pre>
<hr>
<h4>oxide::home &#8212; Returns the Oxide home directory path</h4>
<pre>oxide::home()</pre>
<pre>
/Users/ldaniels/GitHub/oxide/oxide_db
</pre>
<hr>
<h4>oxide::println &#8212; Print line function</h4>
<pre>oxide::println("Hello World")</pre>
<pre>
true
</pre>
<hr>
<h4>oxide::reset &#8212; Clears the scope of all user-defined objects</h4>
<pre>oxide::reset()</pre>
<pre>
true
</pre>
<hr>
<h4>oxide::uuid &#8212; Returns a random 128-bit UUID</h4>
<pre>oxide::uuid()</pre>
<pre>
6a4cb116-8cb5-4f34-b2f8-21bfd780d9d8
</pre>
<hr>
<h4>oxide::version &#8212; Returns the Oxide version</h4>
<pre>oxide::version()</pre>
<pre>
0.35
</pre>
<hr>
<h4>str::ends_with &#8212; Returns true if string `a` ends with string `b`</h4>
<pre>str::ends_with('Hello World', 'World')</pre>
<pre>
true
</pre>
<hr>
<h4>str::format &#8212; Returns an argument-formatted string</h4>
<pre>str::format("This {} the {}", "is", "way")</pre>
<pre>
This is the way
</pre>
<hr>
<h4>str::index_of &#8212; Returns the index of string `b` in string `a`</h4>
<pre>str::index_of('The little brown fox', 'brown')</pre>
<pre>
11
</pre>
<hr>
<h4>str::join &#8212; Combines an array into a string</h4>
<pre>str::join(['1', 5, 9, '13'], ', ')</pre>
<pre>
1, 5, 9, 13
</pre>
<hr>
<h4>str::left &#8212; Returns n-characters from left-to-right</h4>
<pre>str::left('Hello World', 5)</pre>
<pre>
Hello
</pre>
<hr>
<h4>str::len &#8212; Returns the number of characters contained in the string</h4>
<pre>str::len('The little brown fox')</pre>
<pre>
20
</pre>
<hr>
<h4>str::right &#8212; Returns n-characters from right-to-left</h4>
<pre>str::right('Hello World', 5)</pre>
<pre>
World
</pre>
<hr>
<h4>str::split &#8212; Splits string `a` by delimiter string `b`</h4>
<pre>str::split('Hello,there World', ' ,')</pre>
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
<pre>str::starts_with('Hello World', 'World')</pre>
<pre>
false
</pre>
<hr>
<h4>str::strip_margin &#8212; Returns the string with all characters on each line are striped up to the margin character</h4>
<pre>str::strip_margin("
|Code example:
|
|from stocks
|where exchange is 'NYSE'
", '|')</pre>
<pre>

Code example:

from stocks
where exchange is 'NYSE'
</pre>
<hr>
<h4>str::substring &#8212; Returns a substring of string `s` from `m` to `n`</h4>
<pre>str::substring('Hello World', 0, 5)</pre>
<pre>
Hello
</pre>
<hr>
<h4>str::to_string &#8212; Converts a value to its text-based representation</h4>
<pre>str::to_string(125.75)</pre>
<pre>
125.75
</pre>
<hr>
<h4>testing::assert &#8212; Evaluates an assertion returning true or an error</h4>
<pre>import testing
assert(matches(
   [ 1 "a" "b" "c" ],
   [ 1 "a" "b" "c" ]
))</pre>
<pre>
true
</pre>
<hr>
<h4>testing::feature &#8212; Creates a new test feature</h4>
<pre>import testing
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
                })</pre>
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
<h4>testing::matches &#8212; Compares two values</h4>
<pre>import testing::matches
a := { scores: [82, 78, 99], first: "Tom", last: "Lane" }
b := { last: "Lane", first: "Tom", scores: [82, 78, 99] }
matches(a, b)</pre>
<pre>
true
</pre>
<hr>
<h4>testing::type_of &#8212; Returns the type of a value</h4>
<pre>testing::type_of([12, 76, 444])</pre>
<pre>
Array(3)
</pre>
<hr>
<h4>tools::compact &#8212; Shrinks a table by removing deleted rows</h4>
<pre>[+] stocks := ns("platform.compact.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "DMX", exchange: "NYSE", last_sale: 99.99 },
     { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
     { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
     { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
     { symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
     { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 },
     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
[+] delete from stocks where last_sale > 1.0
[+] from stocks</pre>
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
<pre>tools::describe({
   symbol: "BIZ",
   exchange: "NYSE",
   last_sale: 23.66
})</pre>
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
<pre>[+] stocks := ns("platform.fetch.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
     { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
[+] tools::fetch(stocks, 2)</pre>
<pre>
|------------------------------------|
| id | symbol | exchange | last_sale |
|------------------------------------|
| 2  | JET    | NASDAQ   | 32.12     |
|------------------------------------|
</pre>
<hr>
<h4>tools::journal &#8212; Retrieves the journal for an event-source or table function</h4>
<pre>import tools
stocks := ns("platform.journal.stocks")
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
<h4>tools::pop &#8212; Removes and returns a value or object from a Sequence</h4>
<pre>import tools
[+] stocks := ns("platform.pop.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
     { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
[+] stocks::pop(stocks)</pre>
<pre>
Illegal structure platform.pop.stocks
</pre>
<hr>
<h4>tools::push &#8212; Appends a value or object to a Sequence</h4>
<pre>import tools
[+] stocks := ns("platform.push.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
     { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 },
     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }] ~> stocks
[+] stocks::push({ symbol: "XYZ", exchange: "NASDAQ", last_sale: 24.78 })
[+] stocks</pre>
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
<h4>tools::replay &#8212; Reconstructs the state of a journaled table</h4>
<pre>import tools
stocks := ns("platform.table_fn.stocks")
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
<pre>
3
</pre>
<hr>
<h4>tools::reverse &#8212; Returns a reverse copy of a table, string or array</h4>
<pre>import tools
to_table(reverse(
   ['cat', 'dog', 'ferret', 'mouse']
))</pre>
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
<h4>tools::row_id &#8212; Returns the unique ID for the last retrieved row</h4>
<pre>tools::row_id()</pre>
<pre>
0
</pre>
<hr>
<h4>tools::scan &#8212; Returns existence metadata for a table</h4>
<pre>[+] import tools
[+] stocks := ns("platform.scan.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 12.33 },
     { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
     { symbol: "BIZ", exchange: "NYSE", last_sale: 9.775 },
     { symbol: "GOTO", exchange: "OTC", last_sale: 0.1442 },
     { symbol: "XYZ", exchange: "NYSE", last_sale: 0.0289 }] ~> stocks
[+] delete from stocks where last_sale > 1.0
[+] stocks:::scan()</pre>
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
<pre>tools::to_array("Hello")</pre>
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
<pre>import tools::to_csv
[+] stocks := ns("platform.csv.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
     { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
     { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
     { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
     { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
stocks:::to_csv()</pre>
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
<pre>import tools::to_json
[+] stocks := ns("platform.json.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "ABC", exchange: "AMEX", last_sale: 11.11 },
     { symbol: "UNO", exchange: "OTC", last_sale: 0.2456 },
     { symbol: "BIZ", exchange: "NYSE", last_sale: 23.66 },
     { symbol: "GOTO", exchange: "OTC", last_sale: 0.1428 },
     { symbol: "BOOM", exchange: "NASDAQ", last_sale: 0.0872 }] ~> stocks
stocks:::to_json()</pre>
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
<pre>tools::to_table(['cat', 'dog', 'ferret', 'mouse'])</pre>
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
<pre>util::base64('Hello World')</pre>
<pre>
SGVsbG8gV29ybGQ=
</pre>
<hr>
<h4>util::to_binary &#8212; Translates a numeric value into binary</h4>
<pre>util::to_binary(0b1011 & 0b1101)</pre>
<pre>
1001
</pre>
<hr>
<h4>util::gzip &#8212; Compresses bytes via gzip</h4>
<pre>util::gzip('Hello World')</pre>
<pre>
1f8b08000000000000fff348cdc9c95708cf2fca49010056b1174a0b000000
</pre>
<hr>
<h4>util::gunzip &#8212; Decompresses bytes via gzip</h4>
<pre>util::gunzip(util::gzip('Hello World'))</pre>
<pre>
48656c6c6f20576f726c64
</pre>
<hr>
<h4>util::hex &#8212; Translates bytes into hexadecimal</h4>
<pre>util::hex('Hello World')</pre>
<pre>
48656c6c6f20576f726c64
</pre>
<hr>
<h4>util::md5 &#8212; Creates a MD5 digest</h4>
<pre>util::md5('Hello World')</pre>
<pre>
b10a8db164e0754105b7a99be72e3fe5
</pre>
<hr>
<h4>util::to_ascii &#8212; Converts an integer to ASCII</h4>
<pre>util::to_ascii(177)</pre>
<pre>
±
</pre>
<hr>
<h4>util::to_date &#8212; Converts a value to Date</h4>
<pre>util::to_date(177)</pre>
<pre>
1970-01-01T00:00:00.177Z
</pre>
<hr>
<h4>util::to_f32 &#8212; Converts a value to f32</h4>
<pre>util::to_f32(4321)</pre>
<pre>
4321
</pre>
<hr>
<h4>util::to_f64 &#8212; Converts a value to f64</h4>
<pre>util::to_f64(4321)</pre>
<pre>
4321
</pre>
<hr>
<h4>util::to_i8 &#8212; Converts a value to i8</h4>
<pre>util::to_i8(88)</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_i16 &#8212; Converts a value to i16</h4>
<pre>util::to_i16(88)</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_i32 &#8212; Converts a value to i32</h4>
<pre>util::to_i32(88)</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_i64 &#8212; Converts a value to i64</h4>
<pre>util::to_i64(88)</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_i128 &#8212; Converts a value to i128</h4>
<pre>util::to_i128(88)</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_u8 &#8212; Converts a value to u8</h4>
<pre>util::to_u8(88)</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_u16 &#8212; Converts a value to u16</h4>
<pre>util::to_u16(88)</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_u32 &#8212; Converts a value to u32</h4>
<pre>util::to_u32(88)</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_u64 &#8212; Converts a value to u64</h4>
<pre>util::to_u64(88)</pre>
<pre>
88
</pre>
<hr>
<h4>util::to_u128 &#8212; Converts a value to u128</h4>
<pre>util::to_u128(88)</pre>
<pre>
88
</pre>
<hr>
<h4>www::url_decode &#8212; Decodes a URL-encoded string</h4>
<pre>www::url_decode('http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998')</pre>
<pre>
http://shocktrade.com?name=the hero&t=9998
</pre>
<hr>
<h4>www::url_encode &#8212; Encodes a URL string</h4>
<pre>www::url_encode('http://shocktrade.com?name=the hero&t=9998')</pre>
<pre>
http%3A%2F%2Fshocktrade.com%3Fname%3Dthe%20hero%26t%3D9998
</pre>
<hr>
<h4>www::serve &#8212; Starts a local HTTP service</h4>
<pre>[+] www::serve(8822)
[+] stocks := ns("platform.www.quotes")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] [{ symbol: "XINU", exchange: "NYSE", last_sale: 8.11 },
     { symbol: "BOX", exchange: "NYSE", last_sale: 56.88 },
     { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 },
     { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 },
     { symbol: "MIU", exchange: "OTCBB", last_sale: 2.24 }] ~> stocks
GET http://localhost:8822/platform/www/quotes/1/4</pre>
<pre>
|------------------------------------|
| id | exchange | last_sale | symbol |
|------------------------------------|
| 0  | NYSE     | 56.88     | BOX    |
| 1  | NASDAQ   | 32.12     | JET    |
| 2  | AMEX     | 12.49     | ABC    |
|------------------------------------|
</pre>
