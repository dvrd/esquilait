use crate::sqlite::db::Database;

pub fn handle_dot_commands(command: String, db: Database) {
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
            println!("{}", names);
        }
        ".schemas" => {
            let schemas = db.get_schemas_vec();
            println!("{:#?}", schemas);
        }
        _ => println!("unknown command"),
    }
}
