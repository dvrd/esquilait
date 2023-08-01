use anyhow::Result;
use console::Term;
use std::{fs::File, io::Write, process};

use crate::{
    dot_commands::handle_dot_commands,
    repl::{self, Command},
    sqlite::db::Database,
    utils::{elog, log, wlog},
};

pub struct App {
    pub is_running: bool,
    pub term: Term,
    pub db: Option<Database>,
    pub history: Vec<String>,
}

impl App {
    pub fn new() -> Self {
        App {
            db: None,
            term: Term::stdout(),
            is_running: true,
            history: vec![],
        }
    }

    pub fn delete(&mut self, input: &mut String) -> Result<()> {
        if !input.is_empty() {
            input.pop();
            self.term.clear_chars(input.len().saturating_add(1))?;
            self.term.write(format!("{input}").as_bytes())?;
            self.term.flush()?;
        }
        Ok(())
    }

    pub fn write(&mut self, input: &mut String) -> Result<()> {
        self.term.clear_chars(input.len().saturating_sub(1))?;
        self.term.write(format!("{input}").as_bytes())?;
        self.term.flush()?;
        Ok(())
    }

    pub fn previous_command(&mut self) -> Option<String> {
        self.history.pop()
    }

    pub fn router(&mut self, command: Command) -> Result<()> {
        match command.clone() {
            Command::Dot(cmd) => match self.db.as_ref() {
                Some(db) => {
                    handle_dot_commands(cmd, db);
                }
                None => elog("Please, load a database first"),
            },
            Command::Load(path) => match File::open(&path) {
                Ok(file) => match Database::new(file) {
                    Ok(db) => {
                        log(format!("Sucessfully loaded {path}").as_str());
                        self.db = Some(db);
                    }
                    Err(msg) => elog(format!("! {msg}").as_str()),
                },
                Err(msg) => elog(format!("! {msg}").as_str()),
            },
            Command::Sql(cmd) => match self.db.as_ref() {
                Some(db) => {
                    if let Err(msg) = repl::run(cmd, db) {
                        self.router(Command::Error(msg.to_string()))?
                    }
                }
                None => elog("Please, load a database first"),
            },
            Command::Error(msg) => elog(format!("! {msg}").as_str()),
            Command::Quit => self.stop(),
            Command::Unknown => elog("Unknown command"),
            Command::History => match self.previous_command() {
                Some(cmd) => {
                    dbg!(cmd);
                    // self.router(cmd)?
                }
                None => wlog("No previous commands"),
            },
            Command::Utility(shell) => {
                process::Command::new(shell).status()?;
            }
        };

        Ok(())
    }

    fn stop(&mut self) {
        self.is_running = false;
    }
}
