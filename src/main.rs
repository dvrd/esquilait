mod app;
mod dot_commands;
mod parsers;
mod repl;
mod sqlite;
mod utils;

use anyhow::Result;
use console::Key;
use std::io::Write;

use app::App;
use repl::Command;

fn main() -> Result<()> {
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
