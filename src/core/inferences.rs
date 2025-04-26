#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Inferences class
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::expression::Expression::*;
use crate::expression::{DatabaseOps, Expression, Mutations};
use crate::number_kind::NumberKind;
use crate::parameter::Parameter;
use crate::platform::PlatformOps;
use crate::sequences::{Array, Sequence};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Function, PlatformOp};
use std::convert::From;
use std::ops::Deref;

/// Type-Inference Detection
pub struct Inferences;

impl Inferences {
    /// provides type inference for the given [Expression]
    pub fn infer(expr: &Expression) -> DataType {
        match expr {
            ArrayExpression(items) => ArrayType(items.len()),
            AsValue(_, e) => Self::infer(e),
            BitwiseAnd(a, b) => Self::infer_a_or_b(a, b),
            BitwiseOr(a, b) => Self::infer_a_or_b(a, b),
            BitwiseShiftLeft(a, b) => Self::infer_a_or_b(a, b),
            BitwiseShiftRight(a, b) => Self::infer_a_or_b(a, b),
            BitwiseXor(a, b) => Self::infer_a_or_b(a, b),
            CodeBlock(ops) => ops.last().map(Self::infer).unwrap_or(DynamicType),
            ColonColon(a, b) | ColonColonColon(a, b) =>
                match (a.deref(), b.deref()) {
                    (Variable(pkg), FunctionCall { fx, .. }) => match fx.deref() {
                        Variable(name) =>
                            PlatformOps::find_function(pkg, name)
                                .map(|pf| pf.return_type.clone())
                                .unwrap_or(DynamicType),
                        _ => DynamicType,
                    }
                    _ => DynamicType
                }
            Condition(..) => BooleanType,
            CurvyArrowLeft(..) => StructureType(vec![]),
            CurvyArrowRight(..) => BooleanType,
            DatabaseOp(a) => match a {
                DatabaseOps::Queryable(_) => BooleanType,
                DatabaseOps::Mutation(m) => match m {
                    Mutations::Append { .. } => NumberType(NumberKind::RowIdKind),
                    _ => NumberType(NumberKind::RowsAffectedKind),
                }
            }
            Directive(..) => BooleanType,
            Divide(a, b) => Self::infer_a_or_b(a, b),
            ElementAt(..) => DynamicType,
            Factorial(a) => Self::infer(a),
            Feature { .. } => BooleanType,
            FnExpression { params, returns, .. } =>
                FunctionType(params.clone(), Box::from(returns.clone())),
            ForEach(..) => BooleanType,
            From(..) => TableType(vec![], 0),
            FunctionCall { fx, .. } => Self::infer(fx),
            HTTP { .. } => DynamicType,
            If { a: true_v, b: Some(false_v), .. } =>
                Self::infer_alles(vec![true_v, false_v]),
            If { a: true_v, .. } => Self::infer(true_v),
            Import(..) => BooleanType,
            Include(..) => BooleanType,
            Literal(Function { body: code, .. }) => Self::infer(code),
            Literal(PlatformOp(pf)) => pf.get_return_type(),
            Literal(v) => v.get_type(),
            Minus(a, b) => Self::infer_a_or_b(a, b),
            Module(..) => BooleanType,
            Modulo(a, b) => Self::infer_a_or_b(a, b),
            Multiply(a, b) => Self::infer_a_or_b(a, b),
            Neg(a) => Self::infer(a),
            New(a) => Self::infer(a),
            Ns(..) => BooleanType,
            Parameters(params) => ArrayType(params.len()),
            Plus(a, b) => Self::infer_a_or_b(a, b),
            PlusPlus(a, b) => Self::infer_a_or_b(a, b),
            Pow(a, b) => Self::infer_a_or_b(a, b),
            Range(a, b) => Self::infer_a_or_b(a, b),
            Return(a) => Self::infer_all(a),
            Scenario { .. } => BooleanType,
            SetVariable(..) => BooleanType,
            SetVariables(..) => BooleanType,
            StructureExpression(params) => StructureType(params.iter()
                .map(|(name, expr)| Parameter::new(name, Inferences::infer(expr)))
                .collect()),
            TupleExpression(params) => TupleType(params.iter()
                .map(|p| Self::infer(p))
                .collect::<Vec<_>>()),
            TypeDef(expr) => Self::infer(expr),
            Variable(..) => DynamicType,
            Via(..) => TableType(vec![], 0),
            While { .. } => DynamicType,
        }
    }

    /// provides type inference for the given [Expression]s
    fn infer_a_or_b(a: &Expression, b: &Expression) -> DataType {
        Self::infer_best_fit(vec![Self::infer(a), Self::infer(b)])
    }

    /// provides type inference for the given [Expression]s
    fn infer_all(items: &Vec<Expression>) -> DataType {
        Self::infer_best_fit(items.iter()
            .map(Self::infer)
            .collect::<Vec<_>>())
    }

    /// provides type inference for the given [Expression]s
    fn infer_alles(items: Vec<&Expression>) -> DataType {
        Self::infer_best_fit(items.iter()
            .map(|e| *e)
            .map(Self::infer)
            .collect::<Vec<_>>())
    }

    /// provides type inference for the given [TypedValue]
    pub fn infer_array(array: &Array) -> DataType {
        Self::infer_best_fit(array.get_values().iter()
            .map(|v| v.get_type())
            .collect::<Vec<_>>())
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

    /// provides type inference for the given [TypedValue]
    pub fn infer_values(values: &Vec<TypedValue>) -> DataType {
        Self::infer_best_fit(values.iter()
            .map(|v| v.get_type())
            .collect::<Vec<_>>())
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
        );
        assert_eq!(kind, StringType(5))
    }

    #[test]
    fn test_infer_a_or_b_numbers() {
        let kind = Inferences::infer_a_or_b(
            &Literal(Number(I64Value(76))),
            &Literal(Number(F64Value(76.0))),
        );
        assert_eq!(kind, NumberType(F64Kind))
    }

    #[test]
    fn test_infer_all() {
        let kind = Inferences::infer_all(&vec![
            Literal(StringValue("apple".into())),
            Literal(StringValue("banana".into())),
            Literal(StringValue("cherry".into())),
        ]);
        assert_eq!(kind, StringType(6))
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
    fn test_infer_values() {
        let kind = Inferences::infer_values(&vec![
            StringValue("apple".into()),
            StringValue("banana".into()),
            StringValue("cherry".into()),
        ]);
        assert_eq!(kind, StringType(6))
    }
}