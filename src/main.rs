mod tokenizer;
mod dataframes;
mod rows;
mod typed_values;
mod fields;
mod data_types;
mod columns;
mod namespaces;
mod dataframe_config;
mod tokens;
mod field_metadata;
mod row_metadata;
mod table_columns;
mod iocost;
mod codec;
mod testdata;

fn main() {
    println!("Tiny-VM v0.1.0");

    let text = "this\n is\n 1 \n\"way of the world\"\n ! `x` + '%' $";
    match tokenizer::parse_fully(text) {
        Ok(tokens) => {
            for (i, tok) in tokens.iter().enumerate() {
                println!("token[{}]: {:?}", i, tok)
            }
            assert_eq!(tokens.len(), 9);
        }
        Err(err) =>
            eprintln!("Error parsing text: {}", err)
    }
}

