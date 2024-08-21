[+] stocks := ns("interpreter.include.stocks")
[+] table(symbol: String(8), exchange: String(8), last_sale: f64) ~> stocks
[+] append stocks from { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 }
[+] append stocks from { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 }
[+] append stocks from { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }
[+] from stocks
