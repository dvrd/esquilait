use anyhow::{bail, Result};
use console::Key;
use std::ffi::OsStr;
use std::io::Write;
use std::num::NonZeroU64;
use std::path::Path;

use crate::app::App;
use crate::parsers::sql::{Select, SelectColumns};
use crate::sqlite::{
    db::{Database, Search},
    schemas::Schema,
    tables::Table,
};
use crate::utils::{find_table_index, print_rows};

#[derive(Debug, Clone)]
pub enum Command {
    Dot(String),
    Sql(Select),
    Load(String),
    Error(String),
    History,
    Unknown,
    Utility(String),
    Quit,
}

fn get_extension_from_filename(filename: &str) -> Option<&str> {
    Path::new(filename).extension().and_then(OsStr::to_str)
}

static SHELL_TERMS: [&str; 3] = ["ls", "pwd", "clear"];
static QUIT_TERMS: [&str; 4] = ["quit", "q", "Q", "QUIT"];

impl From<&mut String> for Command {
    fn from(value: &mut String) -> Self {
        let words: Vec<_> = value.split_whitespace().collect();

        match words.as_slice() {
            [word] if word.starts_with(".") => Command::Dot(word.to_string()),
            [word] if QUIT_TERMS.contains(word) => Command::Quit,
            [word] if SHELL_TERMS.contains(word) => Command::Utility(word.to_string()),
            _ if value == "\u{b}\n" => Command::History, // Ctrl + k
            [cmd, path] if *cmd == "load" => match get_extension_from_filename(path) {
                Some(ext) if ext == "db" => Command::Load(path.to_string()),
                _ => Command::Error(format!("File \"{path}\" extension extraction failed")),
            },
            [cmd, stmt @ ..] if *cmd == "sql" => match stmt.join(" ").parse() {
                Ok(stmt) => Command::Sql(stmt),
                Err(msg) => Command::Error(msg.to_string()),
            },
            _ => Command::Unknown,
        }
    }
}

pub fn run(stmt: Select, db: &Database) -> Result<()> {
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
                        Search::new(pgno, Some(search_key), None, schema.to_owned(), stmt.conds)
                    }
                    None => {
                        let pgno = match NonZeroU64::new(schema.rootpage) {
                            Some(pgno) => pgno,
                            None => bail!("invalid index rootpage: {}", schema.rootpage),
                        };
                        Search::new(pgno, None, None, schema.to_owned(), stmt.conds)
                    }
                };

            let rows = db.rows(table_search);

            print_rows(rows, columns);
        }
    };

    Ok(())
}

pub fn start() -> Result<()> {
    let mut app = App::new();
    let mut input = String::new();
    let mut next;

    while app.is_running {
        input.clear();
        next = false;
        app.term.write("\n> ".as_bytes())?;

        while !next {
            match app.term.read_key()? {
                Key::Enter => {
                    next = true;
                    app.history.push(input.clone());
                    let command = Command::from(&mut input);
                    println!();
                    app.router(command)?;
                }
                Key::Backspace => {
                    app.delete(&mut input)?;
                }
                Key::ArrowUp => {
                    if let Some(cmd) = app.previous_command() {
                        input = cmd;
                        app.write(&mut format!("> {input}"))?
                    };
                }
                Key::Char(key) => {
                    if key != '\u{b}' {
                        input.push(key);
                        app.write(&mut input)?;
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}
