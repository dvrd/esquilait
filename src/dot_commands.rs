use crate::sqlite::db::Database;

pub fn handle_dot_commands(command: String, db: &Database) {
    let words: Vec<_> = command.split_whitespace().collect();

    match words.as_slice() {
        [word] if *word == ".dbinfo" => {
            println!("{}", db);
        }
        [word] if *word == ".tables" => {
            let schemas = db.get_schemas_vec();
            let names = schemas
                .iter()
                .filter(|s| s.name != "sqlite_sequence")
                .map(|s| s.name.clone())
                .collect::<Vec<String>>()
                .join(" ");
            println!("{}", names);
        }
        [word] if *word == ".schemas" => {
            let schemas = db.get_schemas_vec();
            println!("{:#?}", schemas);
        }
        _ => println!("unknown command"),
    }
}
