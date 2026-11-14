//! Documents as rows and columns (SpelaionDB's ADR-056, point 3).
//!
//! One row per document. The columns are `_id`, then the top-level fields in the order they
//! are first seen in the first batch (at most [`MAX_FIELDS`]), then a last column `…` that is
//! always there. A column's logical type comes from the first batch: every non-null value of
//! one BSON type that maps exactly, otherwise `json`. A field of a later document that has no
//! column, or whose value does not fit its column, goes into `…` as part of a JSON object of
//! that document's leftover fields (its typed cell is null). Nothing is lost, and no column
//! changes type halfway through a result.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder,
};
use mongodb::bson::{Bson, Document};
use spelaion_driver_api::{ColumnMeta, DriverError, LogicalType, Result, ResultSchema};

/// The most field columns a result gets; the rest of each document goes to `…`.
pub const MAX_FIELDS: usize = 256;

/// The name of the last column, where whatever has no column of its own goes.
pub const LEFTOVERS: &str = "…";

/// What a column holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// `int` and `long`.
    Int,
    Float,
    Bool,
    Text,
    /// `date`, as microseconds since 1970 in UTC.
    Date,
    /// `decimal`, as its text.
    Decimal,
    /// `objectId`, as its 24 hexadecimal digits.
    ObjectId,
    /// Anything, as relaxed Extended JSON.
    Json,
}

impl Kind {
    /// The kind a single value would make a column, if all its values were of its type.
    fn of(value: &Bson) -> Option<Kind> {
        match value {
            Bson::Null => None,
            Bson::Int32(_) | Bson::Int64(_) => Some(Kind::Int),
            Bson::Double(_) => Some(Kind::Float),
            Bson::Boolean(_) => Some(Kind::Bool),
            Bson::String(_) => Some(Kind::Text),
            Bson::DateTime(_) => Some(Kind::Date),
            Bson::Decimal128(_) => Some(Kind::Decimal),
            Bson::ObjectId(_) => Some(Kind::ObjectId),
            Bson::Array(_)
            | Bson::Document(_)
            | Bson::RegularExpression(_)
            | Bson::JavaScriptCode(_)
            | Bson::JavaScriptCodeWithScope(_)
            | Bson::Timestamp(_)
            | Bson::Binary(_)
            | Bson::Symbol(_)
            | Bson::Undefined
            | Bson::MaxKey
            | Bson::MinKey
            | Bson::DbPointer(_) => Some(Kind::Json),
        }
    }

    fn logical(self) -> LogicalType {
        match self {
            Kind::Int => LogicalType::Int,
            Kind::Float => LogicalType::Float,
            Kind::Bool => LogicalType::Bool,
            Kind::Text | Kind::ObjectId => LogicalType::Text,
            Kind::Date => LogicalType::TimestampTz,
            Kind::Decimal => LogicalType::Decimal,
            Kind::Json => LogicalType::Json,
        }
    }
}

/// The BSON type's name, as MongoDB's `$type` writes it.
fn type_name(value: &Bson) -> &'static str {
    match value {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Array(_) => "array",
        Bson::Document(_) => "object",
        Bson::Boolean(_) => "bool",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::JavaScriptCode(_) => "javascript",
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope",
        Bson::Int32(_) => "int",
        Bson::Int64(_) => "long",
        Bson::Timestamp(_) => "timestamp",
        Bson::Binary(_) => "binData",
        Bson::ObjectId(_) => "objectId",
        Bson::DateTime(_) => "date",
        Bson::Symbol(_) => "symbol",
        Bson::Decimal128(_) => "decimal",
        Bson::Undefined => "undefined",
        Bson::MaxKey => "maxKey",
        Bson::MinKey => "minKey",
        Bson::DbPointer(_) => "dbPointer",
    }
}

struct Column {
    name: String,
    kind: Kind,
}

/// The columns of a result, decided from its first batch.
pub struct Layout {
    columns: Vec<Column>,
    schema: Arc<ResultSchema>,
}

impl Layout {
    /// The layout `first` (the first batch of a result) gives.
    pub fn from_first(first: &[Document]) -> Layout {
        // Field names in the order they are first seen, `_id` first if any document has one.
        let mut names: Vec<String> = Vec::new();
        if first.iter().any(|d| d.contains_key("_id")) {
            names.push("_id".into());
        }
        for document in first {
            for name in document.keys() {
                if names.len() >= MAX_FIELDS {
                    break;
                }
                // A field called `…` could not be told from the last column: it always goes
                // there, with the other leftovers.
                if name == LEFTOVERS || names.iter().any(|n| n == name) {
                    continue;
                }
                names.push(name.clone());
            }
        }
        let mut columns = Vec::with_capacity(names.len());
        let mut metas = Vec::with_capacity(names.len() + 1);
        for name in names {
            let mut kind: Option<Kind> = None;
            let mut mixed = false;
            let mut native: Option<&'static str> = None;
            for value in first.iter().filter_map(|d| d.get(&name)) {
                if let Some(this) = Kind::of(value) {
                    match kind {
                        None => kind = Some(this),
                        Some(seen) if seen == this => {}
                        Some(_) => mixed = true,
                    }
                }
                if !matches!(value, Bson::Null) {
                    match native {
                        None => native = Some(type_name(value)),
                        Some(seen) if seen == type_name(value) => {}
                        // `int` and `long` together are still one kind of column.
                        Some(_) => native = Some("mixed"),
                    }
                }
            }
            let kind = if mixed {
                Kind::Json
            } else {
                kind.unwrap_or(Kind::Json)
            };
            let native_type = if mixed {
                "mixed"
            } else {
                native.unwrap_or("null")
            };
            metas.push(ColumnMeta {
                name: name.clone(),
                logical: kind.logical(),
                native_type: native_type.into(),
                native_id: None,
                origin: None,
            });
            columns.push(Column { name, kind });
        }
        metas.push(ColumnMeta {
            name: LEFTOVERS.into(),
            logical: LogicalType::Json,
            native_type: "object".into(),
            native_id: None,
            origin: None,
        });
        Layout {
            columns,
            schema: Arc::new(ResultSchema::new(metas)),
        }
    }

    pub fn schema(&self) -> Arc<ResultSchema> {
        self.schema.clone()
    }

    /// `documents` as one record batch of this layout.
    pub fn batch(&self, documents: &[Document]) -> Result<RecordBatch> {
        let rows = documents.len();
        let mut builders: Vec<Builder> = self
            .columns
            .iter()
            .map(|c| Builder::new(c.kind, rows))
            .collect();
        let mut leftovers = StringBuilder::with_capacity(rows, rows * 16);
        for document in documents {
            let mut left = Document::new();
            for (column, builder) in self.columns.iter().zip(builders.iter_mut()) {
                match document.get(&column.name) {
                    None => builder.null(),
                    Some(value) => {
                        if !builder.push(value) {
                            builder.null();
                            left.insert(column.name.clone(), value.clone());
                        }
                    }
                }
            }
            for (name, value) in document {
                if !self.columns.iter().any(|c| &c.name == name) {
                    left.insert(name.clone(), value.clone());
                }
            }
            if left.is_empty() {
                leftovers.append_null();
            } else {
                leftovers.append_value(json(&Bson::Document(left)));
            }
        }
        let mut arrays: Vec<ArrayRef> = builders.into_iter().map(Builder::finish).collect();
        arrays.push(Arc::new(leftovers.finish()));
        RecordBatch::try_new(self.schema.arrow.clone(), arrays)
            .map_err(|e| DriverError::Other(format!("building a batch of documents: {e}")))
    }
}

/// A value as relaxed Extended JSON, the text a `json` cell holds.
pub fn json(value: &Bson) -> String {
    value.clone().into_relaxed_extjson().to_string()
}

enum Builder {
    Int(Int64Builder),
    Float(Float64Builder),
    Bool(BooleanBuilder),
    Text(StringBuilder),
    Date(TimestampMicrosecondBuilder),
    Decimal(StringBuilder),
    ObjectId(StringBuilder),
    Json(StringBuilder),
}

impl Builder {
    fn new(kind: Kind, rows: usize) -> Builder {
        match kind {
            Kind::Int => Builder::Int(Int64Builder::with_capacity(rows)),
            Kind::Float => Builder::Float(Float64Builder::with_capacity(rows)),
            Kind::Bool => Builder::Bool(BooleanBuilder::with_capacity(rows)),
            Kind::Text => Builder::Text(StringBuilder::with_capacity(rows, rows * 16)),
            Kind::Date => {
                Builder::Date(TimestampMicrosecondBuilder::with_capacity(rows).with_timezone("UTC"))
            }
            Kind::Decimal => Builder::Decimal(StringBuilder::with_capacity(rows, rows * 8)),
            Kind::ObjectId => Builder::ObjectId(StringBuilder::with_capacity(rows, rows * 24)),
            Kind::Json => Builder::Json(StringBuilder::with_capacity(rows, rows * 32)),
        }
    }

    fn null(&mut self) {
        match self {
            Builder::Int(b) => b.append_null(),
            Builder::Float(b) => b.append_null(),
            Builder::Bool(b) => b.append_null(),
            Builder::Date(b) => b.append_null(),
            Builder::Text(b) | Builder::Decimal(b) | Builder::ObjectId(b) | Builder::Json(b) => {
                b.append_null()
            }
        }
    }

    /// Appends `value` if it fits this column; `false` (and nothing appended) if it does not.
    fn push(&mut self, value: &Bson) -> bool {
        if matches!(value, Bson::Null) {
            self.null();
            return true;
        }
        match (self, value) {
            (Builder::Int(b), Bson::Int32(v)) => b.append_value(i64::from(*v)),
            (Builder::Int(b), Bson::Int64(v)) => b.append_value(*v),
            (Builder::Float(b), Bson::Double(v)) => b.append_value(*v),
            (Builder::Bool(b), Bson::Boolean(v)) => b.append_value(*v),
            (Builder::Text(b), Bson::String(v)) => b.append_value(v),
            (Builder::Date(b), Bson::DateTime(v)) => match v.timestamp_millis().checked_mul(1000) {
                Some(micros) => b.append_value(micros),
                // Further from 1970 than microseconds reach: kept whole in `…`.
                None => return false,
            },
            (Builder::Decimal(b), Bson::Decimal128(v)) => b.append_value(v.to_string()),
            (Builder::ObjectId(b), Bson::ObjectId(v)) => b.append_value(v.to_hex()),
            (Builder::Json(b), v) => b.append_value(json(v)),
            _ => return false,
        }
        true
    }

    fn finish(self) -> ArrayRef {
        match self {
            Builder::Int(mut b) => Arc::new(b.finish()),
            Builder::Float(mut b) => Arc::new(b.finish()),
            Builder::Bool(mut b) => Arc::new(b.finish()),
            Builder::Date(mut b) => Arc::new(b.finish()),
            Builder::Text(mut b)
            | Builder::Decimal(mut b)
            | Builder::ObjectId(mut b)
            | Builder::Json(mut b) => Arc::new(b.finish()),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int64Array, StringArray};
    use mongodb::bson::{doc, oid::ObjectId};

    use super::*;

    fn texts(batch: &RecordBatch, column: usize) -> Vec<Option<String>> {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a text column");
        (0..array.len())
            .map(|i| (!array.is_null(i)).then(|| array.value(i).to_string()))
            .collect()
    }

    #[test]
    fn columns_come_from_the_first_batch_with_id_first() {
        let first = vec![
            doc! { "name": "a", "_id": 1, "age": 30 },
            doc! { "_id": 2, "city": "Madrid", "name": "b" },
        ];
        let layout = Layout::from_first(&first);
        let names: Vec<_> = layout
            .schema()
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        assert_eq!(names, ["_id", "name", "age", "city", "…"]);
        let logical: Vec<_> = layout.schema().columns.iter().map(|c| c.logical).collect();
        assert_eq!(
            logical,
            [
                LogicalType::Int,
                LogicalType::Text,
                LogicalType::Int,
                LogicalType::Text,
                LogicalType::Json
            ]
        );
    }

    #[test]
    fn a_field_of_several_types_is_json_and_says_mixed() {
        let first = vec![doc! { "v": 1 }, doc! { "v": "one" }, doc! { "v": null }];
        let layout = Layout::from_first(&first);
        let v = &layout.schema().columns[0];
        assert_eq!(
            (v.logical, v.native_type.as_str()),
            (LogicalType::Json, "mixed")
        );
        let batch = layout.batch(&first).unwrap();
        assert_eq!(
            texts(&batch, 0),
            [Some("1".into()), Some("\"one\"".into()), None]
        );
    }

    #[test]
    fn int_and_long_are_one_integer_column() {
        let first = vec![doc! { "n": 1_i32 }, doc! { "n": 5_000_000_000_i64 }];
        let layout = Layout::from_first(&first);
        assert_eq!(layout.schema().columns[0].logical, LogicalType::Int);
        let batch = layout.batch(&first).unwrap();
        let n = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!((n.value(0), n.value(1)), (1, 5_000_000_000));
    }

    #[test]
    fn what_does_not_fit_a_later_batch_goes_to_the_leftovers_and_nothing_is_lost() {
        let layout = Layout::from_first(&[doc! { "_id": 1, "age": 30 }]);
        let later = vec![
            doc! { "_id": 2, "age": "thirty", "ssn": "123" },
            doc! { "_id": 3, "age": 31 },
        ];
        let batch = layout.batch(&later).unwrap();
        let age = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(age.is_null(0));
        assert_eq!(age.value(1), 31);
        let left = texts(&batch, 2);
        assert_eq!(left[0].as_deref(), Some(r#"{"age":"thirty","ssn":"123"}"#));
        assert_eq!(left[1], None);
    }

    #[test]
    fn a_field_called_like_the_last_column_is_a_leftover() {
        let first = vec![doc! { "_id": 1, "…": "x" }];
        let layout = Layout::from_first(&first);
        let names: Vec<_> = layout
            .schema()
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        assert_eq!(names, ["_id", "…"]);
        let batch = layout.batch(&first).unwrap();
        assert_eq!(texts(&batch, 1)[0].as_deref(), Some(r#"{"…":"x"}"#));
    }

    #[test]
    fn at_most_256_field_columns() {
        let mut wide = Document::new();
        for i in 0..300 {
            wide.insert(format!("f{i}"), i);
        }
        let layout = Layout::from_first(std::slice::from_ref(&wide));
        assert_eq!(layout.schema().columns.len(), MAX_FIELDS + 1);
        let batch = layout.batch(&[wide]).unwrap();
        let left = texts(&batch, MAX_FIELDS)[0].clone().unwrap();
        assert!(left.contains("\"f299\":299"), "{left}");
        assert!(!left.contains("\"f0\""), "{left}");
    }

    #[test]
    fn object_ids_are_their_hex_digits_and_nested_values_are_json() {
        let id = ObjectId::parse_str("65a1b2c3d4e5f60718293a4b").unwrap();
        let first = vec![doc! { "_id": id, "tags": ["a", 1], "address": { "city": "Ávila" } }];
        let layout = Layout::from_first(&first);
        let metas = &layout.schema().columns;
        assert_eq!(metas[0].native_type, "objectId");
        assert_eq!(metas[1].native_type, "array");
        assert_eq!(metas[2].native_type, "object");
        let batch = layout.batch(&first).unwrap();
        assert_eq!(
            texts(&batch, 0)[0].as_deref(),
            Some("65a1b2c3d4e5f60718293a4b")
        );
        assert_eq!(texts(&batch, 1)[0].as_deref(), Some(r#"["a",1]"#));
        assert_eq!(texts(&batch, 2)[0].as_deref(), Some(r#"{"city":"Ávila"}"#));
    }

    #[test]
    fn an_empty_result_still_has_its_columns() {
        let layout = Layout::from_first(&[]);
        let names: Vec<_> = layout
            .schema()
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect();
        assert_eq!(names, ["…"]);
        assert_eq!(layout.batch(&[]).unwrap().num_rows(), 0);
    }
}
