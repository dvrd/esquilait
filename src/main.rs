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

    match stmt.columns {
        SelectColumns::Count => {
            let page = match NonZeroU64::new(schema.rootpage) {
                Some(pgno) => db.get_page(pgno)?,
                None => bail!("invalid table rootpage: {}", schema.rootpage),
            };
            println!("{}", page.header.cell_count);
        }
        _ => {
            let columns = table.select(&stmt);

            let table_search =
                match find_table_index(&stmt.conds, stmt.name.as_str(), &table, &schemas) {
                    Some((table_index, search_key)) => {
                        let pgno = match NonZeroU64::new(table_index) {
                            Some(pgno) => pgno,
                            None => bail!("invalid index rootpage: {}", schema.rootpage),
                        };

                        Search {
                            pgno,
                            key: Some(search_key),
                            indeces: None,
                            schema: schema.to_owned(),
                            conds: stmt.conds,
                        }
                    }
                    None => {
                        let pgno = match NonZeroU64::new(schema.rootpage) {
                            Some(pgno) => pgno,
                            None => bail!("invalid index rootpage: {}", schema.rootpage),
                        };
                        Search {
                            pgno,
                            key: None,
                            indeces: None,
                            schema: schema.to_owned(),
                            conds: stmt.conds,
                        }
                    }
                };

            let rows = db.rows(table_search);

            print_rows(rows, columns);
        }
    }

    Ok(())
}
