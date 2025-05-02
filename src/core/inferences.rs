#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Type Inferences class
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::expression::Expression::*;
use crate::expression::{DatabaseOps, Expression, Mutations};
use crate::machine;
use crate::number_kind::NumberKind;
use crate::number_kind::NumberKind::I64Kind;
use crate::parameter::Parameter;
use crate::platform::PlatformOps;
use crate::sequences::Sequence;
use crate::typed_values::TypedValue::{Function, PlatformOp};
use std::convert::From;
use std::ops::Deref;

/// Type-Inference Detection
pub struct Inferences;

impl Inferences {
    /// provides type inference for the given [Expression]
    pub fn infer(expr: &Expression) -> DataType {
        Self::infer_with_hints(expr, &vec![])
    }

    /// provides type inference for the given [Expression] with hints
    /// to improve the matching performance.
    pub fn infer_with_hints(
        expr: &Expression,
        hints: &Vec<Parameter>,
    ) -> DataType {
        let data_type = Self::do_infer_with_hints(expr, hints);
        //println!("infer_with_hints: [{}] {:?} => '{}'", if hints.is_empty() { "N" } else { "Y" }, expr, data_type);
        data_type
    }

    /// provides type inference for the given [Expression] with hints
    /// to improve the matching performance.
    fn do_infer_with_hints(
        expr: &Expression,
        hints: &Vec<Parameter>,
    ) -> DataType {
        match expr {
            ArrayExpression(items) => ArrayType(items.len()),
            // alias functions: name: get_name()
            AsValue(_, e) => Self::infer_with_hints(e, hints),
            BitwiseAnd(a, b) => Self::infer_a_or_b(a, b, hints),
            BitwiseOr(a, b) => Self::infer_a_or_b(a, b, hints),
            BitwiseShiftLeft(a, b) => Self::infer_a_or_b(a, b, hints),
            BitwiseShiftRight(a, b) => Self::infer_a_or_b(a, b, hints),
            BitwiseXor(a, b) => Self::infer_a_or_b(a, b, hints),
            CodeBlock(ops) => ops.last().map(|op| Self::infer_with_hints(op, hints))
                .unwrap_or(DynamicType),
            // platform functions: cal::now()
            ColonColon(a, b) | ColonColonColon(a, b) =>
                match (a.deref(), b.deref()) {
                    (Variable(package), FunctionCall { fx, .. }) =>
                        match fx.deref() {
                            Variable(name) =>
                                PlatformOps::find_function(package, name)
                                    .map(|pf| pf.return_type)
                                    .unwrap_or(DynamicType),
                            _ => DynamicType
                        }
                    _ => DynamicType
                }
            Condition(..) => BooleanType,
            CurvyArrowLeft(..) => StructureType(vec![]),
            CurvyArrowRight(..) => BooleanType,
            DatabaseOp(a) => match a {
                DatabaseOps::Queryable(_) => TableType(vec![], 0),
                DatabaseOps::Mutation(m) => match m {
                    Mutations::Append { .. } => NumberType(NumberKind::RowIdKind),
                    _ => NumberType(NumberKind::RowsAffectedKind),
                }
            }
            Directive(..) => BooleanType,
            Divide(a, b) => Self::infer_a_or_b(a, b, hints),
            ElementAt(..) => DynamicType,
            Factorial(a) => Self::infer_with_hints(a, hints),
            Feature { .. } => BooleanType,
            FnExpression { params, returns, .. } =>
                FunctionType(params.clone(), Box::from(returns.clone())),
            ForEach(..) => DynamicType,
            From(..) => TableType(vec![], 0),
            FunctionCall { fx, .. } => Self::infer_with_hints(fx, hints),
            HTTP { .. } => DynamicType,
            If { a: true_v, b: Some(false_v), .. } => Self::infer_a_or_b(true_v, false_v, hints),
            If { a: true_v, .. } => Self::infer_with_hints(true_v, hints),
            Import(..) => BooleanType,
            Include(..) => BooleanType,
            Literal(Function { body, .. }) => Self::infer_with_hints(body, hints),
            Literal(PlatformOp(pf)) => pf.get_return_type(),
            Literal(v) => v.get_type(),
            Minus(a, b) => Self::infer_a_or_b(a, b, hints),
            Module(..) => BooleanType,
            Modulo(a, b) => Self::infer_a_or_b(a, b, hints),
            Multiply(a, b) => Self::infer_a_or_b(a, b, hints),
            Neg(a) => Self::infer_with_hints(a, hints),
            New(a) => Self::infer_with_hints(a, hints),
            Ns(..) => BooleanType,
            Parameters(params) => ArrayType(params.len()),
            Plus(a, b) => Self::infer_a_or_b(a, b, hints),
            PlusPlus(a, b) => Self::infer_a_or_b(a, b, hints),
            Pow(a, b) => Self::infer_a_or_b(a, b, hints),
            Range(a, b) => Self::infer_a_or_b(a, b, hints),
            Return(a) => Self::infer_with_hints(a, hints),
            Scenario { .. } => BooleanType,
            SetVariable(..) => BooleanType,
            SetVariables(..) => BooleanType,
            // structures: { symbol: "ABC", exchange: "AMEX", last_sale: 12.49 }
            StructureExpression(key_values) => {
                let mut params = vec![];
                let mut combined_hints = hints.clone();
                for (name, value) in key_values {
                    let param = Parameter::new(name.clone(), Self::infer_with_hints(value, &combined_hints));
                    params.push(param.clone());
                    combined_hints.push(param);
                }
                StructureType(params)
            }
            // tuples: (100, 23, 36)
            TupleExpression(values) => TupleType(values.iter()
                .map(|p| Self::infer_with_hints(p, hints))
                .collect::<Vec<_>>()),
            TypeDef(expr) => Self::infer_with_hints(expr, hints),
            Variable(name) =>
                match name {
                    s if s == machine::ROW_ID => NumberType(I64Kind),
                    _ =>
                        match hints.iter().find(|hint| hint.get_name() == name) {
                            Some(param) => param.get_data_type(),
                            None => DynamicType
                        }
                }
            Via(..) => TableType(vec![], 0),
            While { .. } => DynamicType,
        }
    }

    /// provides type inference for the given [Expression]s
    fn infer_a_or_b(
        a: &Expression,
        b: &Expression,
        hints: &Vec<Parameter>,
    ) -> DataType {
        Self::infer_best_fit(vec![
            Self::infer_with_hints(a, hints),
            Self::infer_with_hints(b, hints)
        ])
    }

    /// provides type resolution for the given [Vec<DataType>]
    fn infer_best_fit(types: Vec<DataType>) -> DataType {
        fn larger(a: &usize, b: &usize) -> usize {
            (if a > b { a } else { b }).to_owned()
        }

        match types.len() {
            0 => DynamicType,
            1 => types[0].to_owned(),
            _ => types[1..].iter().fold(types[0].to_owned(), |agg, t|
                match (agg, t) {
                    (BinaryType(a), BinaryType(b)) => StringType(larger(&a, b)),
                    (StringType(a), StringType(b)) => StringType(larger(&a, b)),
                    (_, t) => t.to_owned()
                })
        }
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::number_kind::NumberKind::{F64Kind, I64Kind};
    use crate::numbers::Numbers::{F64Value, I64Value};
    use crate::testdata::{verify_bit_operator, verify_data_type, verify_math_operator};
    use crate::typed_values::TypedValue::{Number, StringValue};

    #[test]
    fn test_infer() {
        let kind = Inferences::infer(
            &Literal(StringValue("hello world".into()))
        );
        assert_eq!(kind, StringType(11))
    }

    #[test]
    fn test_infer_a_or_b_strings() {
        let kind = Inferences::infer_a_or_b(
            &Literal(StringValue("yes".into())),
            &Literal(StringValue("hello".into())),
            &vec![],
        );
        assert_eq!(kind, StringType(5))
    }

    #[test]
    fn test_infer_a_or_b_numbers() {
        let kind = Inferences::infer_a_or_b(
            &Literal(Number(I64Value(76))),
            &Literal(Number(F64Value(76.0))),
            &vec![],
        );
        assert_eq!(kind, NumberType(F64Kind))
    }

    #[test]
    fn test_infer_best_fit() {
        let kind = Inferences::infer_best_fit(vec![
            StringType(11),
            StringType(110),
            StringType(55)
        ]);
        assert_eq!(kind, StringType(110))
    }

    #[test]
    fn test_infer_conditionals_and() {
        verify_data_type("true && false", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_between() {
        verify_data_type("20 between 1 and 20", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_betwixt() {
        verify_data_type("20 betwixt 1 and 21", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_eq() {
        verify_data_type("x == y", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_gt() {
        verify_data_type("x > y", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_gte() {
        verify_data_type("x >= y", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_is() {
        verify_data_type("a is b", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_isnt() {
        verify_data_type("a isnt b", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_lt() {
        verify_data_type("x < y", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_lte() {
        verify_data_type("x <= y", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_neq() {
        verify_data_type("x != y", BooleanType);
    }

    #[test]
    fn test_infer_conditionals_or() {
        verify_data_type("true || false", BooleanType);
    }

    #[test]
    fn test_infer_mathematics_divide() {
        verify_math_operator("/");
    }

    #[test]
    fn test_infer_mathematics_minus() {
        verify_math_operator("-");
    }

    #[test]
    fn test_infer_mathematics_plus() {
        verify_math_operator("+");
    }

    #[test]
    fn test_infer_mathematics_plus_plus() {
        verify_math_operator("++");
    }

    #[test]
    fn test_infer_mathematics_power() {
        verify_math_operator("**");
    }

    #[test]
    fn test_infer_mathematics_shl() {
        verify_bit_operator("<<");
    }

    #[test]
    fn test_infer_mathematics_shr() {
        verify_bit_operator(">>");
    }

    #[test]
    fn test_infer_mathematics_times() {
        verify_math_operator("*");
    }

    #[test]
    fn test_infer_return() {
        verify_data_type("return 5", NumberType(I64Kind));
    }

    #[test]
    fn test_infer_row_id() {
        verify_data_type("__row_id__", NumberType(I64Kind));
    }

    #[test]
    fn test_infer_tools_row_id() {
        verify_data_type("tools::row_id()", NumberType(I64Kind));
    }
}