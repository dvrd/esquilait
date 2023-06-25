mod sqlite;

use anyhow::{bail, Result};
use std::fs::File;
use std::num::NonZeroU64;

use sqlite::{
    parser::{self, SelectColumns},
    schemas::{Schema, SchemaType},
    tables::Table,
    Database, Search,
};

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

    match command.as_str() {
        ".dbinfo" => {
            println!("{}", db);
        }
        ".tables" => {
            let schemas = db.get_schemas_vec();
            let names = schemas
                .iter()
                .filter(|s| s.name != "sqlite_sequence")
                .map(|s| s.name.clone())
                .collect::<Vec<String>>()
                .join(" ");
            print!("{}", names);
        }
        ".schemas" => {
            let schemas = db.get_schemas_vec();
            println!("{:#?}", schemas);
        }
        query => {
            let stmt: parser::Select = query.parse()?;
            let schemas = db.get_schemas();
            let (schema, table): (&Schema, Table) = match schemas.get(&stmt.name) {
                Some(s) => (s, s.try_into()?),
                None => bail!("no such table: {}", stmt.name),
            };
            let columns = table.select(&stmt);

            let (table_index, search_key) = if let Some(cond) = &stmt.cond {
                match cond {
                    parser::Condition::Eq(col_name, search_key) => {
                        let indexable_col = table.columns.get(&col_name.clone()).unwrap();
                        let index = schemas
                            .iter()
                            .filter(|(_, s)| {
                                s.stype == SchemaType::Index && s.table_name == stmt.name
                            })
                            .find(|(_, s)| {
                                let column = match parser::create_idx_sql(&s.sql) {
                                    Ok((_, column)) => column,
                                    Err(_) => return false,
                                };
                                column == indexable_col.name
                            })
                            .map(|(_, s)| s.rootpage);

                        let search_key = match index {
                            Some(_) => Some(search_key),
                            None => None,
                        };

                        (index, search_key)
                    }
                }
            } else {
                (None, None)
            };

            match stmt.columns {
                SelectColumns::Count => {
                    let page = db
                        .get_page(NonZeroU64::new(schema.rootpage).unwrap())
                        .unwrap();
                    println!("{}", page.header.cell_count);
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
                        key: search_key.cloned(),
                        indeces: None,
                        schema: schema.clone(),
                        cond: stmt.cond.clone(),
                    };

                    let rows = db.rows(table_search);

                    for row in rows {
                        println!(
                            "{}",
                            indices
                                .iter()
                                .map(|&i| row[i].to_string())
                                .collect::<Vec<String>>()
                                .join("|")
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
