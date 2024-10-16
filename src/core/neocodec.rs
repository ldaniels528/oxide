////////////////////////////////////////////////////////////////////
// Codec class - responsible for encoding/decoding bytes
////////////////////////////////////////////////////////////////////

use shared_lib::fail;

use crate::errors::Errors::Exact;
use crate::expression::Expression;
use crate::expression::Expression::Literal;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::ErrorValue;

/// Oxide Codec
pub struct Codec;

impl Codec {
    pub fn decode(bytes: &Vec<u8>) -> Expression {
        Self::unwrap_as_value(bincode::deserialize(bytes),
                              |err| Literal(ErrorValue(Exact(err.to_string()))))
    }

    pub fn encode(model: &Expression) -> std::io::Result<Vec<u8>> {
        Self::unwrap_as_result(bincode::serialize(model))
    }

    pub fn decode_value(bytes: &Vec<u8>) -> TypedValue {
        Self::unwrap_as_value(bincode::deserialize(bytes),
                              |err| ErrorValue(Exact(err.to_string())))
    }

    pub fn encode_value(model: &TypedValue) -> std::io::Result<Vec<u8>> {
        Self::unwrap_as_result(bincode::serialize(model))
    }

    pub fn unwrap_as_result<T>(result: bincode::Result<T>) -> std::io::Result<T> {
        match result {
            Ok(bytes) => Ok(bytes),
            Err(err) => fail(err.to_string())
        }
    }

    pub fn unwrap_as_value<T>(result: bincode::Result<T>, f: fn(bincode::Error) -> T) -> T {
        result.unwrap_or_else(|err| f(err))
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::expression::Conditions::{Equal, GreaterThan, LessOrEqual};
    use crate::expression::Expression::{Condition, If, JSONExpression, Literal, Multiply, Plus, Quarry, Variable, Via};
    use crate::expression::{Excavation, Mutation, Queryable};
    use crate::model_row_collection::ModelRowCollection;
    use crate::neocodec::Codec;
    use crate::numbers::NumberValue::{F64Value, I64Value};
    use crate::testdata::{make_quote, make_quote_columns};
    use crate::typed_values::TypedValue::{Number, StringValue, TableValue};

    #[test]
    fn test_table_codec() {
        let phys_columns = make_quote_columns();
        let expected = TableValue(ModelRowCollection::from_rows(phys_columns.clone(), vec![
            make_quote(0, "ABC", "AMEX", 11.77),
            make_quote(1, "UNO", "OTC", 0.2456),
            make_quote(2, "BIZ", "NYSE", 23.66),
            make_quote(3, "GOTO", "OTC", 0.1428),
            make_quote(4, "BOOM", "NASDAQ", 56.87)]));
        let bytes = Codec::encode_value(&expected).unwrap();
        let actual = Codec::decode_value(&bytes);
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_if_else() {
        let model = If {
            condition: Box::new(Condition(GreaterThan(
                Box::new(Variable("num".into())),
                Box::new(Literal(Number(I64Value(25)))),
            ))),
            a: Box::new(Literal(StringValue("Yes".into()))),
            b: Some(Box::new(Literal(StringValue("No".into())))),
        };

        let bytes = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&bytes);
        assert_eq!(actual, model)
    }

    #[test]
    fn test_expression_delete() {
        let model = Quarry(Excavation::Mutate(Mutation::Delete {
            path: Box::new(Variable("stocks".into())),
            condition: Some(LessOrEqual(
                Box::new(Variable("last_sale".into())),
                Box::new(Literal(Number(F64Value(1.0)))),
            )),
            limit: Some(Box::new(Literal(Number(I64Value(100))))),
        }));
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_literal() {
        let model = Literal(StringValue("hello".into()));
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_variable() {
        let model = Variable("name".into());
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_math_ops() {
        let model = Plus(Box::new(Literal(Number(I64Value(2)))),
                         Box::new(Multiply(Box::new(Literal(Number(I64Value(4)))),
                                           Box::new(Literal(Number(I64Value(3)))))));
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_select() {
        let model = Quarry(Excavation::Query(Queryable::Select {
            fields: vec![Variable("symbol".into()), Variable("exchange".into()), Variable("last_sale".into())],
            from: Some(Box::new(Variable("stocks".into()))),
            condition: Some(LessOrEqual(
                Box::new(Variable("last_sale".into())),
                Box::new(Literal(Number(F64Value(1.0)))),
            )),
            group_by: None,
            having: None,
            order_by: Some(vec![Variable("symbol".into())]),
            limit: Some(Box::new(Literal(Number(I64Value(5))))),
        }));
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_update() {
        let model = Quarry(Excavation::Mutate(Mutation::Update {
            path: Box::new(Variable("stocks".into())),
            source: Box::new(Via(Box::new(JSONExpression(vec![
                ("last_sale".into(), Literal(Number(F64Value(0.1111)))),
            ])))),
            condition: Some(Equal(
                Box::new(Variable("exchange".into())),
                Box::new(Literal(StringValue("NYSE".into()))),
            )),
            limit: Some(Box::new(Literal(Number(I64Value(10))))),
        }));
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }

    #[test]
    fn test_json_literal() {
        let model = JSONExpression(vec![
            ("symbol".to_string(), Literal(StringValue("TRX".into()))),
            ("exchange".to_string(), Literal(StringValue("NYSE".into()))),
            ("last_sale".to_string(), Literal(Number(F64Value(11.1111)))),
        ]);
        let byte_code = Codec::encode(&model).unwrap();
        let actual = Codec::decode(&byte_code);
        assert_eq!(actual, model);
    }
}