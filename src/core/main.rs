////////////////////////////////////////////////////////////////////
//      TinyDB Server v0.1.0
////////////////////////////////////////////////////////////////////

use std::io;
use std::ops::Deref;

use actix_web::{HttpResponse, Responder, web};
use serde::{Deserialize, Serialize};

use crate::dataframe_config::DataFrameConfig;
use crate::dataframes::DataFrame;
use crate::namespaces::Namespace;
use crate::rows::Row;
use crate::server::{ColumnJs, RowJs, ServerError, SystemInfoJs};

mod codec;
mod columns;
mod compiler;
mod dataframes;
mod dataframe_config;
mod data_types;
mod expression;
mod field_metadata;
mod fields;
mod machine;
mod namespaces;
mod opcode;
mod row_collection;
mod row_metadata;
mod rows;
mod server;
mod table_columns;
mod table_view;
mod template;
mod testdata;
mod token_slice;
mod tokenizer;
mod tokens;
mod typed_values;

#[actix_web::main]
async fn main() -> Result<(), ServerError> {
    println!("Welcome to TinyDB Server.\n");
    match server::get_port_number(std::env::args().collect()).await {
        Ok(port) => {
            println!("Starting server on port {}.", port);
            listen_to(&port).await
        }
        Err(err) => Err(err.into())
    }
}

async fn listen_to(port: &str) -> Result<(), ServerError> {
    let server = actix_web::HttpServer::new(|| {
        actix_web::App::new()
            .route("/", web::get().to(index))
            .route("/{database}/{schema}/{name}/{id}", web::get().to(get_row))
            .route("/{database}/{schema}/{name}", web::get().to(get_config))
    })
        .bind(crate::server::get_address(port))?
        .run();
    server.await?;
    Ok(())
}

/// handler function for the index route
// ex: http://localhost:8080/
async fn index() -> impl Responder {
    HttpResponse::Ok().json(SystemInfoJs::new())
}

/// handler function for configuration by namespace (database, schema, name)
// ex: http://localhost:8080/dataframes/create/quotes
async fn get_config(path: web::Path<(String, String, String)>) -> impl Responder {
    let (database, schema, name) = (&path.0, &path.1, &path.2);
    match DataFrameConfig::load(&Namespace::new(database, schema, name)) {
        Ok(cfg) => HttpResponse::Ok().json(cfg),
        Err(err) => {
            eprintln!("error {}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}

/// handler function for row by namespace (database, schema, name) and row ID
// ex: http://localhost:8080/dataframes/create/quotes/0
async fn get_row(path: web::Path<(String, String, String, usize)>) -> impl Responder {
    fn load_row(path: web::Path<(String, String, String, usize)>) -> io::Result<Option<Row>> {
        let (database, schema, name, id) = (&path.0, &path.1, &path.2, path.3);
        let df = DataFrame::load(Namespace::new(database, schema, name))?;
        let (row, metadata) = df.read_row(id)?;
        Ok(if metadata.is_allocated { Some(row) } else { None })
    }

    fn to_json(row: Row) -> RowJs {
        RowJs::new(row.id, row.fields.iter().zip(&row.columns)
            .map(|(f, c)| ColumnJs::new(&c.name, f.value.to_json())).collect())
    }

    match load_row(path) {
        Ok(Some(row)) => HttpResponse::Ok().json(to_json(row)),
        Ok(None) => HttpResponse::Ok().json(serde_json::json!({})),
        Err(err) => {
            eprintln!("{}", err.to_string());
            HttpResponse::InternalServerError().finish()
        }
    }
}
