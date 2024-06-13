drop table ns("interpreter.include.stocks")

shall create table ns("interpreter.include.stocks") (
    symbol: String(8),
    exchange: String(8),
    last_sale: f64
)

shall append ns("interpreter.include.stocks")
                from { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 }

shall append ns("interpreter.include.stocks")
                from { symbol: "BOOM", exchange: "NYSE", last_sale: 56.88 }

shall append ns("interpreter.include.stocks")
                from { symbol: "JET", exchange: "NASDAQ", last_sale: 32.12 }

shall from ns("interpreter.include.stocks")
