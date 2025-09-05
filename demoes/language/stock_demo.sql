//////////////////////////////////////////////////////////////////////////////////////
//      Stock Server Demo
//
//  include "./demoes/language/stock_demo.sql"
//////////////////////////////////////////////////////////////////////////////////////

let port = 8888

http::start(port, {
    '/api/quote' : {
        'GET' : (ticker -> {
            let namespace = 'stock_demo.dev.stocks'
            let stocks = tables::load(namespace)
            stocks where symbol is ticker
        })
        'POST' : (quote -> {
            let namespace = 'stock_demo.dev.stocks'
            let stocks = tables::load(namespace)
            quote ~> stocks
        })
    }
});

feature 'unit_tests' {

    scenario 'shared_state' {
        use oxide
        let port = 8888
        let host = '0.0.0.0'
        let namespace = 'stock_demo.dev.stocks'
    }

    // test 'setup' in 'unit_tests'
    scenario 'setup' inherits 'shared_state' {
        let stocks = Table(
            symbol: String(8),
            exchange: Enum(AMEX, NASDAQ, NYSE, OTCBB, OTHER_OTC),
            last_sale: f64
        )::new::save_as(namespace);

        |------------------------------------|
        | id | symbol | exchange | last_sale |
        |------------------------------------|
        | 0  | ABC    | AMEX     | 11.11     |
        | 1  | UNO    | OTCBB    | 0.2456    |
        | 2  | BIZ    | NYSE     | 23.66     |
        | 3  | XYZ    | AMEX     | 0.1428    |
        | 4  | BOOM   | NASDAQ   | 0.0872    |
        |------------------------------------|
            ~> stocks
    }

    // test ['setup', 'test_get_quote'] in 'unit_tests'
    scenario 'test_get_quote' inherits 'shared_state' {
        let quote = GET 'http://%s:%d/api/quote?ticker=ABC'::sprintf(host, port)
        println("quote = %s"::sprintf(quote))
        assert quote is [{'exchange':'AMEX','last_sale':11.11,'symbol':'ABC'}]
    }
}
