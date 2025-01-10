#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Inferences class
////////////////////////////////////////////////////////////////////

use crate::arrays::Array;
use crate::data_types::DataType;
use crate::data_types::DataType::*;
use crate::expression::Expression::*;
use crate::expression::{DatabaseOps, Expression, Mutations};
use crate::number_kind::NumberKind;
use crate::platform::PlatformOps;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::{Function, PlatformOp};
use std::ops::Deref;

/// Type-Inference Detection
pub struct Inferences;

impl Inferences {
    /// provides type inference for the given [Expression]
    pub fn infer(expr: &Expression) -> DataType {
        match expr {
            ArrayExpression(items) => ArrayType(items.len()),
            AsValue(_, e) => Inferences::infer(e),
            BitwiseAnd(a, b) => Inferences::infer_a_or_b(a, b),
            BitwiseOr(a, b) => Inferences::infer_a_or_b(a, b),
            BitwiseShiftLeft(a, b) => Inferences::infer_a_or_b(a, b),
            BitwiseShiftRight(a, b) => Inferences::infer_a_or_b(a, b),
            BitwiseXor(a, b) => Inferences::infer_a_or_b(a, b),
            CodeBlock(ops) => ops.last().map(Inferences::infer).unwrap_or(VaryingType(vec![])),
            Condition(..) => BooleanType,
            Directive(..) => NumberType(NumberKind::AckKind),
            Divide(a, b) => Inferences::infer_a_or_b(a, b),
            ElementAt(..) => VaryingType(vec![]),
            Extraction(a, b) => match (a.deref(), b.deref()) {
                (Variable(pkg), FunctionCall { fx, .. }) => match fx.deref() {
                    Variable(name) =>
                        PlatformOps::find_function(pkg, name)
                            .map(|pf| pf.return_type.clone())
                            .unwrap_or(VaryingType(vec![])),
                    _ => VaryingType(vec![]),
                }
                _ => VaryingType(vec![])
            }
            ExtractPostfix(..) => VaryingType(vec![]),
            Factorial(a) => Inferences::infer(a),
            Feature { .. } => NumberType(NumberKind::AckKind),
            ForEach(..) => NumberType(NumberKind::AckKind),
            From(..) => TableType(vec![], 0),
            Tuple(..) => FunctionType(vec![]),
            FunctionCall { fx, .. } => Inferences::infer(fx),
            HTTP { .. } => VaryingType(vec![]),
            If { a: true_v, b: Some(false_v), .. } =>
                Inferences::infer_alles(vec![true_v, false_v]),
            If { a: true_v, .. } => Inferences::infer(true_v),
            Import(..) => NumberType(NumberKind::AckKind),
            Include(..) => NumberType(NumberKind::AckKind),
            JSONExpression(..) => StructureType(Vec::new()), // TODO can we infer?
            Literal(Function { code, .. }) => Inferences::infer(code),
            Literal(PlatformOp(pf)) => pf.get_return_type(),
            Literal(v) => v.get_type(),
            Minus(a, b) => Inferences::infer_a_or_b(a, b),
            Module(..) => NumberType(NumberKind::AckKind),
            Modulo(a, b) => Inferences::infer_a_or_b(a, b),
            Multiply(a, b) => Inferences::infer_a_or_b(a, b),
            Neg(a) => Inferences::infer(a),
            Ns(..) => NumberType(NumberKind::AckKind),
            Parameters(params) => ArrayType(params.len()),
            Plus(a, b) => Inferences::infer_a_or_b(a, b),
            PlusPlus(a, b) => Inferences::infer_a_or_b(a, b),
            Pow(a, b) => Inferences::infer_a_or_b(a, b),
            DatabaseOp(a) => match a {
                DatabaseOps::Queryable(_) => NumberType(NumberKind::AckKind),
                DatabaseOps::Mutation(m) => match m {
                    Mutations::Append { .. } => NumberType(NumberKind::RowIdKind),
                    _ => NumberType(NumberKind::RowsAffectedKind),
                }
            }
            Range(a, b) => Inferences::infer_a_or_b(a, b),
            Return(a) => Self::infer_all(a),
            Scenario { .. } => NumberType(NumberKind::AckKind),
            SetVariable(..) => NumberType(NumberKind::AckKind),
            Variable(..) => VaryingType(vec![]),
            Via(..) => TableType(vec![], 0),
            While { .. } => VaryingType(vec![]),
        }
    }

    /// provides type inference for the given [Expression]s
    fn infer_a_or_b(a: &Expression, b: &Expression) -> DataType {
        Self::infer_best_fit(vec![Self::infer(a), Self::infer(b)])
    }

    /// provides type inference for the given [Expression]s
    fn infer_all(items: &Vec<Expression>) -> DataType {
        Self::infer_best_fit(items.iter()
            .map(Inferences::infer)
            .collect::<Vec<_>>())
    }

    /// provides type inference for the given [Expression]s
    fn infer_alles(items: Vec<&Expression>) -> DataType {
        Self::infer_best_fit(items.iter()
            .map(|e| *e)
            .map(Inferences::infer)
            .collect::<Vec<_>>())
    }

    /// provides type inference for the given [TypedValue]
    pub fn infer_array(array: &Array) -> DataType {
        Self::infer_best_fit(array.values().iter()
            .map(|v| v.get_type())
            .collect::<Vec<_>>())
    }

    /// provides type resolution for the given [Vec<DataType>]
    fn infer_best_fit(types: Vec<DataType>) -> DataType {
        fn larger(a: &usize, b: &usize) -> usize {
            (if a > b { a } else { b }).to_owned()
        }

        let result = match types.len() {
            0 => VaryingType(vec![]),
            1 => types[0].to_owned(),
            _ => types[1..].iter().fold(types[0].to_owned(), |agg, t|
                match (agg, t) {
                    (VaryingType(a), b) => VaryingType({
                        let mut c = a.clone();
                        c.push(b.clone());
                        c
                    }),
                    (b, VaryingType(a)) => VaryingType({
                        let mut c = a.clone();
                        c.push(b.clone());
                        c
                    }),
                    (BinaryType(a), BinaryType(b)) => StringType(larger(&a, b)),
                    (StringType(a), StringType(b)) => StringType(larger(&a, b)),
                    (_, t) => t.to_owned()
                })
        };

        // final massaging
        match result {
            VaryingType(a) if a.len() == 1 => a[0].clone(),
            z => z
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