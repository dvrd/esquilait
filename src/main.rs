mod app;
mod dot_commands;
mod parsers;
mod repl;
mod sqlite;
mod utils;

use anyhow::{bail, Result};

use app::App;
use repl::Command;

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();

    match args.len() {
        2 => bail!("Missing <command>"),
        3 => {
            let mut app = App::new();
            let db_file_path = args[1].to_string();
            app.router(Command::Load(db_file_path))?;
            println!();
            let stmt = args[2].parse()?;
            app.router(Command::Sql(stmt))?;
        }
        _ => repl::start()?,
    }

    Ok(())
}
