Tiny VM (Rust variant)
======================

The purpose of this project is evaluate the developer experience offered by
Rust and Go by implementing the same small virtual machine in both languages.

You can find the [Go variant here](https://github.com/ldaniels528/tiny_vm.go).

I'm just getting started bu check back from time to time to see the progress.

#### Test input

```textmate
this\n is\n 1 \n\"way of the world\"\n - `x` + '%'
```

#### Test output

```textmate
token[0]: AlphaNumeric("this", 0, 4, 1, 1)
token[1]: AlphaNumeric("is", 6, 8, 2, 4)
token[2]: Numeric("1", 10, 11, 3, 7)
token[3]: DoubleQuoted("\"way of the world\"", 13, 31, 4, 9)
token[4]: Symbol("-", 33, 34, 5, 28)
token[5]: BackticksQuoted("`x`", 35, 38, 5, 30)
token[6]: Symbol("+", 39, 40, 5, 34)
token[7]: SingleQuoted("'%'", 41, 44, 5, 36)
```