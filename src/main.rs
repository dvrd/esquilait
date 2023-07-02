mod dot_commands;
mod parsers;
mod sqlite;
mod utils;

use anyhow::{bail, Ok, Result};
use dot_commands::handle_dot_commands;
use std::fs::File;
use std::num::NonZeroU64;

use parsers::sql::{Select, SelectColumns};
use sqlite::{
    db::{Database, Search},
    schemas::Schema,
    tables::Table,
};
use utils::{find_table_index, print_rows};

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let file = File::open(&args[1])?;
    let command = &args[2];
    let db = Database::new(file)?;

    let is_dot_command = command.starts_with(".");

    if is_dot_command {
        handle_dot_commands(command.to_owned(), db);
        return Ok(());
    }

    let stmt: Select = command.parse()?;
    let schemas = db.get_schemas();
    let (schema, table): (&Schema, Table) = match schemas.get(&stmt.name) {
        Some(s) => (s, s.try_into()?),
        None => bail!("no such table: {}", stmt.name),
    };
    let columns = table.select(&stmt);

    let (table_index, search_key) = match &stmt.cond {
        Some(cond) => find_table_index(cond, stmt.name.as_str(), &table, &schemas),
        None => (None, None),
    };

    match stmt.columns {
        SelectColumns::Count => {
            let pgno = NonZeroU64::new(schema.rootpage)
                .unwrap_or_else(|| panic!("invalid table rootpage: {}", schema.rootpage));
            let page = db.get_page(pgno)?;
            println!("{}", page.header.cell_count);
            Ok(())
        }
        _ => {
            let indices: Vec<usize> = columns.iter().map(|c| c.idx).collect();

            let pgno = NonZeroU64::new(match table_index {
                Some(pgno) => pgno,
                None => schema.rootpage,
            })
            .unwrap_or_else(|| panic!("invalid index rootpage: {}", schema.rootpage));

            let table_search = Search {
                pgno,
                key: search_key,
                indeces: None,
                schema: schema.to_owned(),
                cond: stmt.cond,
            };

            let rows = db.rows(table_search);

            print_rows(rows, indices);
            Ok(())
        }
    }
}
