use colored::Colorize;
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
    conds: &Vec<Condition>,
    target: &str,
    table: &Table,
    schemas: &HashMap<String, Schema>,
) -> Option<(u64, String)> {
    for cond in conds {
        let (col_name, search_key) = cond.unbox();
        if let Some(indexable_col) = table.columns.get(col_name) {
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

            if let Some(idx) = index {
                let sk = search_key.to_owned();
                return Some((idx, sk));
            }
        };
    }
    None
}

fn truncate(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        None => s,
        Some((idx, _)) => &s[..idx],
    }
}

pub fn print_rows(rows: Vec<Row>, columns: Vec<Column>) {
    use prettytable::{Cell, Row, Table};
    let mut table = Table::new();

    table.add_row(Row::new(
        columns.iter().map(|c| Cell::new(c.name.as_str())).collect(),
    ));
    for row in rows.clone() {
        table.add_row(Row::new(
            columns
                .iter()
                .map(|c| {
                    let text = row[c.idx].to_string();
                    if text.len() > 10 && columns.len() > 4 {
                        let text = truncate(&text, 10);
                        let text = text.to_string() + "...";
                        return Cell::new(&text[..]);
                    }
                    Cell::new(&text)
                })
                .collect(),
        ));
    }
    table.printstd();
    println!("Number of rows: {}", rows.len());
}

pub fn log(msg: &str) {
    eprint!("{} {}", "∆".blue(), msg.blue());
}

pub fn elog(msg: &str) {
    eprint!("{} {}", "∆(e)".red(), msg.red());
}

pub fn wlog(msg: &str) {
    eprint!("{} {}", "∆(w)".yellow(), msg.yellow());
}
