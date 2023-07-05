use std::collections::HashMap;

use crate::{
    parsers::sql::{create_idx_sql, Condition},
    sqlite::{
        db::Row,
        schemas::{Schema, SchemaType},
        tables::{Column, Table},
    },
};

pub fn find_table_index(
    cond: &Condition,
    target: &str,
    table: &Table,
    schemas: &HashMap<String, Schema>,
) -> (Option<u64>, Option<String>) {
    match cond {
        Condition::Eq(col_name, search_key) => {
            let indexable_col = table.columns.get(col_name);
            match indexable_col {
                Some(indexable_col) => {
                    let index = schemas
                        .iter()
                        .filter(|(_, s)| s.stype == SchemaType::Index && s.table_name == target)
                        .find(|(_, s)| {
                            let column = match create_idx_sql(&s.sql) {
                                Ok((_, column)) => column,
                                Err(_) => return false,
                            };
                            column == indexable_col.name
                        })
                        .map(|(_, s)| s.rootpage);

                    let search_key = match index {
                        Some(_) => Some(search_key.to_owned()),
                        None => None,
                    };

                    (index, search_key)
                }
                None => (None, None),
            }
        }
    }
}

pub fn print_rows(rows: Vec<Row>, columns: Vec<Column>) {
    println!(
        "{}",
        columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<&str>>()
            .join("|")
    );
    for row in rows {
        println!(
            "{}",
            columns
                .iter()
                .map(|c| row[c.idx].to_string())
                .collect::<Vec<String>>()
                .join("|")
        );
    }
}
