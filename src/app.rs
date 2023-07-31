use anyhow::Result;
use std::{
    collections::HashMap,
    fs::File,
    io::{self, Stdin, Stdout},
};

use crate::{
    dot_commands::handle_dot_commands,
    repl::{self, Command},
    sqlite::db::Database,
    utils::{elog, log, wlog},
};

pub struct App {
    pub is_running: bool,
    pub stdin: Stdin,
    pub stdout: Stdout,
    pub db: Option<Database>,
    pub history: Vec<Command>,
}

impl App {
    pub fn new() -> Self {
        App {
            db: None,
            stdin: io::stdin(),
            stdout: io::stdout(),
            is_running: true,
            history: vec![],
        }
    }

    fn previous_command(&mut self) -> Option<Command> {
        self.history.pop()
    }

    pub fn router(&mut self, command: Command) -> Result<()> {
        match command.clone() {
            Command::Dot(cmd) => match self.db.as_ref() {
                Some(db) => {
                    handle_dot_commands(cmd, db);
                    self.history.push(command);
                }
                None => elog("Please, load a database first"),
            },
            Command::Load(path) => match File::open(&path) {
                Ok(file) => match Database::new(file) {
                    Ok(db) => {
                        log(format!("Sucessfully loaded {path}").as_str());
                        self.db = Some(db);
                        self.history.push(command);
                    }
                    Err(msg) => elog(format!("! {msg}").as_str()),
                },
                Err(msg) => elog(format!("! {msg}").as_str()),
            },
            Command::Sql(cmd) => match self.db.as_ref() {
                Some(db) => {
                    if let Err(msg) = repl::run(cmd, db) {
                        self.router(Command::Error(msg.to_string()))?
                    } else {
                        self.history.push(command);
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
        };

        Ok(())
    }

    fn stop(&mut self) {
        self.is_running = false;
    }
}
