////////////////////////////////////////////////////////////////////
// Inferences class
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType::*;
use crate::data_types::{DataType, StorageTypes};
use crate::expression::Expression::*;
use crate::expression::{BitwiseOps, Excavation, Expression, Mutation};
use crate::outcomes::OutcomeKind;
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
            ArrayExpression(items) => ArrayType(Box::new(Inferences::infer_all(items))),
            AsValue(_, e) => Inferences::infer(e),
            BitwiseOp(bwo) => match bwo {
                BitwiseOps::And(a, b) => Inferences::infer_a_or_b(a, b),
                BitwiseOps::Or(a, b) => Inferences::infer_a_or_b(a, b),
                BitwiseOps::ShiftLeft(a, b) => Inferences::infer_a_or_b(a, b),
                BitwiseOps::ShiftRight(a, b) => Inferences::infer_a_or_b(a, b),
                BitwiseOps::Xor(a, b) => Inferences::infer_a_or_b(a, b),
            }
            CodeBlock(ops) => ops.last().map(Inferences::infer).unwrap_or(LazyType),
            Condition(..) => BooleanType,
            Directive(..) => OutcomeType(OutcomeKind::Acked),
            Divide(a, b) => Inferences::infer_a_or_b(a, b),
            ElementAt(..) => LazyType,
            Extraction(a, b) => match (a.deref(), b.deref()) {
                (Variable(pkg), FunctionCall { fx, .. }) => match fx.deref() {
                    Variable(name) =>
                        PlatformOps::find_function(pkg, name)
                            .map(|pf| pf.return_type.clone())
                            .unwrap_or(LazyType),
                    _ => LazyType,
                }
                _ => LazyType
            }
            ExtractPostfix(..) => LazyType,
            Factorial(a) => Inferences::infer(a),
            Feature { .. } => OutcomeType(OutcomeKind::Acked),
            From(..) => TableType(vec![], StorageTypes::BLOB),
            FunctionCall { fx, .. } => Inferences::infer(fx),
            HTTP { .. } => LazyType,
            If { a: true_v, b: Some(false_v), .. } =>
                Inferences::infer_alles(vec![true_v, false_v]),
            If { a: true_v, .. } => Inferences::infer(true_v),
            Import(..) => OutcomeType(OutcomeKind::Acked),
            Include(..) => OutcomeType(OutcomeKind::Acked),
            JSONExpression(..) => StructureType(Vec::new()), // TODO can we infer?
            Literal(Function { code, .. }) => Inferences::infer(code),
            Literal(PlatformOp(pf)) => pf.get_return_type(),
            Literal(v) => v.get_type(),
            Minus(a, b) => Inferences::infer_a_or_b(a, b),
            Module(..) => OutcomeType(OutcomeKind::Acked),
            Modulo(a, b) => Inferences::infer_a_or_b(a, b),
            Multiply(a, b) => Inferences::infer_a_or_b(a, b),
            Neg(a) => Inferences::infer(a),
            Ns(..) => OutcomeType(OutcomeKind::Acked),
            Parameters(params) => ArrayType(Box::new(StructureType(params.to_owned()))),
            Plus(a, b) => Inferences::infer_a_or_b(a, b),
            Pow(a, b) => Inferences::infer_a_or_b(a, b),
            Quarry(a) => match a {
                Excavation::Construct(_) => OutcomeType(OutcomeKind::Acked),
                Excavation::Query(_) => OutcomeType(OutcomeKind::Acked),
                Excavation::Mutate(m) => match m {
                    Mutation::Append { .. } => OutcomeType(OutcomeKind::RowInserted),
                    _ => OutcomeType(OutcomeKind::RowsUpdated),
                }
            }
            Range(a, b) => Inferences::infer_a_or_b(a, b),
            Return(a) => Self::infer_all(a),
            Scenario { .. } => OutcomeType(OutcomeKind::Acked),
            SetVariable(..) => OutcomeType(OutcomeKind::Acked),
            Variable(..) => LazyType,
            Via(..) => LazyType,
            While { .. } => LazyType,
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

    /// provides type resolution for the given [Vec<DataType>]
    fn infer_best_fit(types: Vec<DataType>) -> DataType {
        fn larger(a: &StorageTypes, b: &StorageTypes) -> StorageTypes {
            (if a > b { a } else { b }).to_owned()
        }

        match types.len() {
            0 => LazyType,
            1 => types[0].to_owned(),
            _ => types[1..].iter().fold(types[0].to_owned(), |agg, t|
                match (agg, t) {
                    (LazyType, b) => b.to_owned(),
                    (a, LazyType) => a.to_owned(),
                    (ByteArrayType(a), ByteArrayType(b)) => StringType(larger(&a, b)),
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
    use crate::compiler::Compiler;
    use crate::data_types::StorageTypes::FixedSize;
    use crate::number_kind::NumberKind::{F64Kind, I64Kind};
    use crate::numbers::Numbers::{F64Value, I64Value};
    use crate::typed_values::TypedValue::{Number, StringValue};

    #[test]
    fn test_infer() {
        let kind = Inferences::infer(
            &Literal(StringValue("hello world".into()))
        );
        assert_eq!(kind, StringType(FixedSize(11)))
    }

    #[test]
    fn test_infer_a_or_b_strings() {
        let kind = Inferences::infer_a_or_b(
            &Literal(StringValue("yes".into())),
            &Literal(StringValue("hello".into())),
        );
        assert_eq!(kind, StringType(FixedSize(5)))
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
        assert_eq!(kind, StringType(FixedSize(6)))
    }

    #[test]
    fn test_infer_best_fit() {
        let kind = Inferences::infer_best_fit(vec![
            StringType(FixedSize(11)),
            StringType(FixedSize(110)),
            StringType(FixedSize(55))
        ]);
        assert_eq!(kind, StringType(FixedSize(110)))
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
        assert_eq!(kind, StringType(FixedSize(6)))
    }

    fn verify_data_type(code: &str, expected: DataType) {
        let model = Compiler::build(code).unwrap();
        assert_eq!(Inferences::infer(&model), expected);
    }

    fn verify_bit_operator(op: &str) {
        verify_data_type(format!("5 {} 9", op).as_str(), NumberType(I64Kind));
        verify_data_type(format!("a {} b", op).as_str(), LazyType);
    }

    fn verify_math_operator(op: &str) {
        verify_data_type(format!("5 {} 9", op).as_str(), NumberType(I64Kind));
        verify_data_type(format!("9.4 {} 3.7", op).as_str(), NumberType(F64Kind));
        verify_data_type(format!("a {} b", op).as_str(), LazyType);
    }
}