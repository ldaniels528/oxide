////////////////////////////////////////////////////////////////////
// view row-collection module
////////////////////////////////////////////////////////////////////

use crate::expression::Expression;
use crate::field_metadata::FieldMetadata;
use crate::machine::Machine;
use crate::row_collection::RowCollection;
use crate::row_metadata::RowMetadata;
use crate::rows::Row;
use crate::table_columns::TableColumn;
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::ErrorValue;

/// Represents a logical [RowCollection] implementation
#[derive(Debug)]
pub struct ViewRowCollection {
    host: Box<dyn RowCollection>,
    condition: Expression,
}

impl ViewRowCollection {
    pub fn new(host: Box<dyn RowCollection>, condition: Expression) -> Self {
        Self { host, condition }
    }

    pub fn get_condition(&self) -> &Expression { &self.condition }

    pub fn get_host(&self) -> &Box<dyn RowCollection> { &self.host }
}

impl RowCollection for ViewRowCollection {
    fn get_columns(&self) -> &Vec<TableColumn> { self.host.get_columns() }

    fn get_record_size(&self) -> usize { self.host.get_record_size() }

    fn len(&self) -> std::io::Result<usize> { self.host.len() }

    fn overwrite_field(
        &mut self,
        _id: usize,
        _column_id: usize,
        _new_value: TypedValue,
    ) -> TypedValue {
        ErrorValue("Write operations are not allowed.".into())
    }

    fn overwrite_field_metadata(
        &mut self,
        _id: usize,
        _column_id: usize,
        _metadata: FieldMetadata,
    ) -> TypedValue {
        ErrorValue("Write operations are not allowed.".into())
    }

    fn overwrite_row(&mut self, _id: usize, _row: Row) -> TypedValue {
        ErrorValue("Write operations are not allowed.".into())
    }

    fn overwrite_row_metadata(&mut self, _id: usize, _metadata: RowMetadata) -> TypedValue {
        ErrorValue("Write operations are not allowed.".into())
    }

    fn read_field(&self, id: usize, column_id: usize) -> TypedValue {
        self.host.read_field(id, column_id)
    }

    fn read_field_metadata(
        &self,
        id: usize,
        column_id: usize,
    ) -> std::io::Result<FieldMetadata> {
        self.host.read_field_metadata(id, column_id)
    }

    fn read_one(&self, id: usize) -> std::io::Result<Option<Row>> {
        let (row, rmd) = self.host.read_row(id)?;
        if rmd.is_allocated {
            let machine = Machine::new().with_row(&row);
            if row.matches(&machine, &Some(Box::new(self.condition.to_owned()))) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn read_row(&self, id: usize) -> std::io::Result<(Row, RowMetadata)> {
        let (row, rmd) = self.host.read_row(id)?;
        if rmd.is_allocated {
            let machine = Machine::new().with_row(&row);
            if row.matches(&machine, &Some(Box::new(self.condition.to_owned()))) {
                return Ok((row, rmd));
            }
        }
        Ok((Row::empty(row.get_columns()), rmd.with_allocated(false)))
    }

    fn read_row_metadata(&self, id: usize) -> std::io::Result<RowMetadata> {
        let (row, rmd) = self.host.read_row(id)?;
        if rmd.is_allocated {
            let machine = Machine::new().with_row(&row);
            if row.matches(&machine, &Some(Box::new(self.condition.to_owned()))) {
                return Ok(rmd);
            }
        }
        Ok(rmd.with_allocated(false))
    }

    fn resize(&mut self, _new_size: usize) -> TypedValue {
        ErrorValue("Views cannot be resized.".into())
    }
}

// Unit tests
#[cfg(test)]
mod tests {
    use crate::expression::Expression::{Equal, GreaterThan, LessThan, Literal, Variable};
    use crate::machine::Machine;
    use crate::model_row_collection::ModelRowCollection;
    use crate::numbers::NumberValue::Float64Value;
    use crate::row_collection::RowCollection;
    use crate::table_columns::TableColumn;
    use crate::testdata::{make_quote, make_quote_columns};
    use crate::typed_values::TypedValue::{Number, StringValue};
    use crate::view_row_collection::ViewRowCollection;

    #[test]
    fn test_simple_view_condition() {
        let mrc = create_data_set();
        let vrc = ViewRowCollection::new(Box::new(mrc), Equal(
            Box::new(Variable("exchange".into())),
            Box::new(Literal(StringValue("OTC".into())))),
        );

        let rows = vrc.read_active_rows().unwrap();
        Machine::show(rows.to_owned());

        let results = rows.to_owned().iter()
            .map(|row| row.get_values())
            .collect::<Vec<_>>();

        assert_eq!(results, vec![
            vec![StringValue("UNO".into()), StringValue("OTC".into()), Number(Float64Value(0.2456))],
            vec![StringValue("GOTO".into()), StringValue("OTC".into()), Number(Float64Value(0.1442))],
        ])
    }

    #[test]
    fn test_compound_view_condition() {
        let mrc = create_data_set();
        let vrc = ViewRowCollection::new(Box::new(mrc), Equal(
            Box::new(Variable("exchange".into())),
            Box::new(Literal(StringValue("NYSE".into())))),
        );

        let rows = vrc.filter_rows(&LessThan(
            Box::new(Variable("last_sale".into())),
            Box::new(Literal(Number(Float64Value(20.)))),
        )).unwrap();
        Machine::show(rows.to_owned());

        let results = rows.to_owned().iter()
            .map(|row| row.get_values())
            .collect::<Vec<_>>();

        assert_eq!(results, vec![
            vec![StringValue("BIZ".into()), StringValue("NYSE".into()), Number(Float64Value(9.775))],
            vec![StringValue("XYZ".into()), StringValue("NYSE".into()), Number(Float64Value(0.0289))],
        ])
    }

    #[test]
    fn test_out_of_band_rows() {
        let vrc = ViewRowCollection::new(Box::new(create_data_set()), Equal(
            Box::new(Variable("exchange".into())),
            Box::new(Literal(StringValue("NASDAQ".into())))),
        );

        // attempt to retrieve out-of-band rows
        let in_band = vrc.iter()
            .map(|row| row.get_id())
            .collect::<Vec<_>>();
        for id in vrc.get_indices().unwrap() {
            let row = vrc.read_one(id).unwrap();
            assert_eq!(row.is_some(), in_band.contains(&id))
        }
    }

    fn create_data_set() -> ModelRowCollection {
        let columns = make_quote_columns();
        let phys_columns = TableColumn::from_columns(&columns).unwrap();
        ModelRowCollection::from_rows(vec![
            make_quote(0, &phys_columns, "IBM", "NYSE", 21.22),
            make_quote(1, &phys_columns, "ATT", "NYSE", 98.44),
            make_quote(2, &phys_columns, "HOCK", "AMEX", 0.0076),
            make_quote(3, &phys_columns, "XIE", "NASDAQ", 33.33),
            make_quote(4, &phys_columns, "AAA", "NYSE", 22.44),
            make_quote(5, &phys_columns, "XYZ", "NASDAQ", 66.67),
            make_quote(6, &phys_columns, "SSO", "NYSE", 123.44),
            make_quote(7, &phys_columns, "RAND", "AMEX", 11.33),
            make_quote(8, &phys_columns, "ABC", "AMEX", 12.33),
            make_quote(9, &phys_columns, "UNO", "OTC", 0.2456),
            make_quote(10, &phys_columns, "BIZ", "NYSE", 9.775),
            make_quote(11, &phys_columns, "GOTO", "OTC", 0.1442),
            make_quote(12, &phys_columns, "XYZ", "NYSE", 0.0289),
        ])
    }
}