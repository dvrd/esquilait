mod app;
mod dot_commands;
mod parsers;
mod repl;
mod sqlite;
mod utils;

use anyhow::Result;

use app::App;
use repl::Command;

use std::io::{BufRead, Write};

fn main() -> Result<()> {
    let mut app = App::new();
    let input = &mut String::new();

    while app.is_running {
        input.clear();
        print!("> ");

        app.stdout.flush()?;

        let mut handle = app.stdin.lock();

        handle.read_line(input)?;

        let command = Command::from(input.clone());

        app.router(command)?;
    }

    Ok(())
}
