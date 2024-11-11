////////////////////////////////////////////////////////////////////
// Inferences class
////////////////////////////////////////////////////////////////////

use crate::data_types::DataType::*;
use crate::data_types::{DataType, StorageTypes};
use crate::expression::Expression::*;
use crate::expression::{BitwiseOps, Excavation, Expression, Mutation};
use crate::outcomes::OutcomeKind;
use crate::typed_values::TypedValue;

/// Type-Inference Detection
pub struct Inferences;

impl Inferences {
    /// provides type inference for the given [Expression]
    pub fn infer_expr(expr: &Expression) -> DataType {
        match expr {
            ArrayExpression(items) => ArrayType(Box::new(Inferences::infer_vec(items))),
            AsValue(_, e) => Inferences::infer_expr(e),
            BitwiseOp(bwo) => match bwo {
                BitwiseOps::And(a, b) => Inferences::infer_a_or_b(a, b),
                BitwiseOps::Or(a, b) => Inferences::infer_a_or_b(a, b),
                BitwiseOps::ShiftLeft(a, b) => Inferences::infer_a_or_b(a, b),
                BitwiseOps::ShiftRight(a, b) => Inferences::infer_a_or_b(a, b),
                BitwiseOps::Xor(a, b) => Inferences::infer_a_or_b(a, b),
            },
            CodeBlock(..) => LazyEvalType,
            Parameters(params) => ArrayType(Box::new(StructureType(params.to_owned()))),
            Condition(..) => BooleanType,
            Directive(..) => OutcomeType(OutcomeKind::Acked),
            Divide(a, b) => Inferences::infer_a_or_b(a, b),
            ElementAt(..) => LazyEvalType,
            Extraction(..) => LazyEvalType,
            ExtractPostfix(..) => LazyEvalType,
            Factorial(a) => Inferences::infer_expr(a),
            Feature { .. } => OutcomeType(OutcomeKind::Acked),
            From(..) => LazyEvalType,
            FunctionCall { .. } => LazyEvalType,
            HTTP { .. } => LazyEvalType,
            If { a, .. } => Inferences::infer_expr(a),
            Import(..) => OutcomeType(OutcomeKind::Acked),
            Include(..) => OutcomeType(OutcomeKind::Acked),
            JSONExpression(..) => StructureType(Vec::new()), // todo can we infer?
            Literal(v) => v.get_type(),
            Minus(a, b) => Inferences::infer_a_or_b(a, b),
            Module(..) => OutcomeType(OutcomeKind::Acked),
            Modulo(a, b) => Inferences::infer_a_or_b(a, b),
            Multiply(a, b) => Inferences::infer_a_or_b(a, b),
            Neg(a) => Inferences::infer_expr(a),
            Ns(..) => OutcomeType(OutcomeKind::Acked),
            Plus(a, b) => Inferences::infer_a_or_b(a, b),
            Pow(a, b) => Inferences::infer_a_or_b(a, b),
            Quarry(a) => match a {
                Excavation::Construct(_) => OutcomeType(OutcomeKind::Acked),
                Excavation::Query(_) => OutcomeType(OutcomeKind::Acked),
                Excavation::Mutate(m) => match m {
                    Mutation::Append { .. } => OutcomeType(OutcomeKind::RowInserted),
                    _ => OutcomeType(OutcomeKind::RowsUpdated),
                }
            },
            Range(a, b) => Inferences::infer_a_or_b(a, b),
            Return(..) => LazyEvalType,
            Scenario { .. } => OutcomeType(OutcomeKind::Acked),
            SetVariable(..) => OutcomeType(OutcomeKind::Acked),
            Variable(..) => LazyEvalType,
            Via(..) => LazyEvalType,
            While { .. } => LazyEvalType,
        }
    }

    /// provides type inference for the given [Expression]s
    fn infer_a_or_b(a: &Expression, b: &Expression) -> DataType {
        Self::infer_type(vec![Self::infer_expr(a), Self::infer_expr(b)])
    }

    /// provides type resolution for the given [Vec<DataType>]
    pub fn infer_type(types: Vec<DataType>) -> DataType {
        fn larger(a: &StorageTypes, b: &StorageTypes) -> StorageTypes {
            if a > b { a.to_owned() } else { b.to_owned() }
        }

        match types.len() {
            0 => LazyEvalType,
            1 => types[0].to_owned(),
            _ => types[1..].iter().fold(types[0].to_owned(), |agg, t|
                match (agg, t) {
                    (LazyEvalType, b) => b.to_owned(),
                    (a, LazyEvalType) => a.to_owned(),
                    (ByteArrayType(a), ByteArrayType(b)) => StringType(larger(&a, b)),
                    (StringType(a), StringType(b)) => StringType(larger(&a, b)),
                    (_, t) => t.to_owned()
                })
        }
    }

    /// provides type inference for the given [TypedValue]
    pub fn infer_values(items: &Vec<TypedValue>) -> DataType {
        Self::infer_type(items.iter()
            .map(|tv| tv.get_type())
            .collect::<Vec<_>>())
    }

    /// provides type inference for the given [Expression]s
    pub fn infer_vec(items: &Vec<Expression>) -> DataType {
        Self::infer_type(items.iter()
            .map(Inferences::infer_expr)
            .collect::<Vec<_>>())
    }
}

/// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_types::StorageTypes::FixedSize;
    use crate::typed_values::TypedValue::StringValue;

    #[test]
    fn test_infer_expr() {
        let kind = Inferences::infer_expr(
            &Literal(StringValue("hello world".into()))
        );
        assert_eq!(kind, StringType(FixedSize(11)))
    }

    #[test]
    fn test_infer_a_or_b() {
        let kind = Inferences::infer_a_or_b(
            &Literal(StringValue("yes".into())),
            &Literal(StringValue("hello".into())),
        );
        assert_eq!(kind, StringType(FixedSize(5)))
    }

    #[test]
    fn test_infer_type() {
        let kind = Inferences::infer_type(vec![
            StringType(FixedSize(11)),
            StringType(FixedSize(110)),
            StringType(FixedSize(55))
        ]);
        assert_eq!(kind, StringType(FixedSize(110)))
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

    #[test]
    fn test_infer_vec() {
        let kind = Inferences::infer_vec(&vec![
            Literal(StringValue("apple".into())),
            Literal(StringValue("banana".into())),
            Literal(StringValue("cherry".into())),
        ]);
        assert_eq!(kind, StringType(FixedSize(6)))
    }
}