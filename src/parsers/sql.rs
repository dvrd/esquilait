use std::str::FromStr;
use std::{cmp::Ordering, collections::HashMap};

use crate::sqlite::tables::{CellType, Column, Table};

use super::value::Value;

#[derive(Debug, PartialEq, Clone)]
pub enum SelectColumns {
    Columns(Vec<String>),
    All,
    Count,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Condition {
    Eq(String, String),
    Greater(String, String),
    GreaterEq(String, String),
    Less(String, String),
    LessEq(String, String),
    Ne(String, String),
    Between(String, String, String),
}

impl Condition {
    pub fn unbox(&self) -> (&String, &String) {
        match self {
            Condition::Eq(col_name, val) => (col_name, val),
            Condition::Greater(col_name, val) => (col_name, val),
            Condition::GreaterEq(col_name, val) => (col_name, val),
            Condition::Less(col_name, val) => (col_name, val),
            Condition::LessEq(col_name, val) => (col_name, val),
            Condition::Ne(col_name, val) => (col_name, val),
            Condition::Between(col_name, from, _) => (col_name, from), // TODO: Handle this case
        }
    }

    pub fn eval(&self, row: &Vec<Value>, columns: &HashMap<String, Column>) -> bool {
        match self {
            Condition::Eq(col_name, val) => {
                if let Some(column) = columns.get(col_name) {
                    return match &row[column.idx] {
                        Value::Integer(n) => {
                            if let Ok(val) = val.parse::<i64>() {
                                return *n == val;
                            }
                            false
                        }
                        Value::Text(t) => t.cmp(val) == Ordering::Equal,
                        Value::Float(f) => {
                            if let Ok(val) = val.parse::<f64>() {
                                return *f == val;
                            }
                            false
                        }
                        Value::Null => val == "NULL",
                        Value::Blob(_) => false,
                    };
                }

                false
            }
            Condition::GreaterEq(col_name, val) => {
                if let Some(column) = columns.get(col_name) {
                    return match &row[column.idx] {
                        Value::Integer(n) => {
                            if let Ok(val) = val.parse::<i64>() {
                                return *n >= val;
                            }
                            false
                        }
                        Value::Text(t) => {
                            let comparison = t.cmp(val);
                            comparison == Ordering::Greater || comparison == Ordering::Equal
                        }
                        Value::Float(f) => {
                            if let Ok(val) = val.parse::<f64>() {
                                return *f >= val;
                            }
                            false
                        }
                        Value::Null => val == "NULL",
                        Value::Blob(_) => false,
                    };
                }

                false
            }
            Condition::Greater(col_name, val) => {
                if let Some(column) = columns.get(col_name) {
                    return match &row[column.idx] {
                        Value::Integer(n) => {
                            if let Ok(val) = val.parse::<i64>() {
                                return *n > val;
                            }
                            false
                        }
                        Value::Text(t) => t.cmp(val) == Ordering::Greater,
                        Value::Float(f) => {
                            if let Ok(val) = val.parse::<f64>() {
                                return *f > val;
                            }
                            false
                        }
                        Value::Null => val == "NULL",
                        Value::Blob(_) => false,
                    };
                }
                false
            }
            Condition::LessEq(col_name, val) => {
                if let Some(column) = columns.get(col_name) {
                    return match &row[column.idx] {
                        Value::Integer(n) => {
                            if let Ok(val) = val.parse::<i64>() {
                                return *n <= val;
                            }
                            false
                        }
                        Value::Text(t) => {
                            let comparison = t.cmp(val);
                            comparison == Ordering::Less || comparison == Ordering::Equal
                        }
                        Value::Float(f) => {
                            if let Ok(val) = val.parse::<f64>() {
                                return *f <= val;
                            }
                            false
                        }
                        Value::Null => val == "NULL",
                        Value::Blob(_) => false,
                    };
                }

                false
            }
            Condition::Less(col_name, val) => {
                if let Some(column) = columns.get(col_name) {
                    return match &row[column.idx] {
                        Value::Integer(n) => {
                            if let Ok(val) = val.parse::<i64>() {
                                return *n < val;
                            }
                            false
                        }
                        Value::Text(t) => t.cmp(val) == Ordering::Less,
                        Value::Float(f) => {
                            if let Ok(val) = val.parse::<f64>() {
                                return *f < val;
                            }
                            false
                        }
                        Value::Null => val == "NULL",
                        Value::Blob(_) => false,
                    };
                }
                false
            }
            Condition::Ne(col_name, val) => {
                if let Some(column) = columns.get(col_name) {
                    return match &row[column.idx] {
                        Value::Integer(n) => {
                            if let Ok(val) = val.parse::<i64>() {
                                return *n != val;
                            }
                            false
                        }
                        Value::Text(t) => t.cmp(val) != Ordering::Equal,
                        Value::Float(f) => {
                            if let Ok(val) = val.parse::<f64>() {
                                return *f != val;
                            }
                            false
                        }
                        Value::Null => val == "NULL",
                        Value::Blob(_) => false,
                    };
                }

                false
            }
            Condition::Between(col_name, from, to) => {
                if let Some(column) = columns.get(col_name) {
                    return match &row[column.idx] {
                        Value::Integer(n) => {
                            if let (Ok(from), Ok(to)) = (from.parse::<i64>(), to.parse::<i64>()) {
                                return *n >= from && *n <= to;
                            }
                            false
                        }
                        Value::Text(t) => {
                            (t.cmp(from) == Ordering::Greater || t.cmp(from) == Ordering::Equal)
                                && (t.cmp(to) == Ordering::Less || t.cmp(to) == Ordering::Equal)
                        }
                        Value::Float(f) => {
                            if let (Ok(from), Ok(to)) = (from.parse::<f64>(), to.parse::<f64>()) {
                                return *f >= from && *f <= to;
                            }
                            false
                        }
                        _ => false,
                    };
                }

                false
            }
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Select {
    pub name: String,
    pub columns: SelectColumns,
    pub conds: Vec<Condition>,
}

impl FromStr for Select {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match sql::select(s) {
            Ok(s) => Ok(s),
            Err(e) => {
                return Err(anyhow::anyhow!("({})", e));
            }
        }
    }
}

peg::parser! {
  grammar sql() for str {
    pub rule select() -> Select
        =  operation() _ columns:columns() _ ("from" / "FROM") _ name:name() _ conds:search()? {
            Select {
                name: name.to_string(),
                columns: match columns[..] {
                    ["*"] => SelectColumns::All,
                    ["count(*)"] => SelectColumns::Count,
                    _ => SelectColumns::Columns(columns.iter().map(|c| c.to_string()).collect()),
                },
                conds: match conds {
                    Some(conds) => conds,
                    None => vec![]
                }
            }
        }
        / expected!("select")

    pub rule create() -> Table
        =  operation() _ name:name() _ "(" _ columns:column() ** "," _ ")" {
            let mut key: Option<String> = None;
            let columns: Vec<_> = columns.iter().enumerate().map(|(idx, (n, t, pk, auto_inc, nullable))| {
                if *pk {
                    key = Some(n.to_string());
                }
                let name = n.to_string();
                (name.clone(), Column {
                    name,
                    cell_type: *t,
                    idx,
                    pk: *pk,
                    nullable: *nullable,
                })
            }).collect();
            Table {
                name: name.to_string(),
                columns: HashMap::from_iter(columns),
                key,
            }
        }

    pub rule create_idx() -> (String, String)
        =  operation() _ name() _ ("on" / "ON") _ table:name() _ "(" _ column:name() _ ")" {
            (table.to_string(), column.to_string())
        }

    rule _() = quiet!{[' ' | '\t' | '\r' | '\n']*}
        / expected!("whitespace")

    rule word() = ['a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' ]+

    rule name() -> &'input str
        =  quiet!{_ c:$word() { c }}
        / quiet!{_ "\"" c:$( word() ** _) "\"" { c }}
        / expected!("column_name")

    rule value() -> &'input str
        =  quiet!{v:$word() { v }}
        / quiet!{['"' | '\''] v:$(word() ** _) ['"' | '\''] { v }}
        / expected!("value")

    rule search() -> Vec<Condition>
        =  _ ("WHERE" / "where") cond:(condition() ** conditionals()) { cond }
        / expected!("search")

    rule conditionals() -> ()
        = quiet!{"AND" / "and" / "OR" / "or"}
        / expected!("conditionals")

    rule condition() -> Condition
        = quiet!{
            c:name() _ "=" _ v:value() _? { Condition::Eq(c.to_string(), v.to_string()) }
            / c:name() _ ">=" _ v:value() _? { Condition::GreaterEq(c.to_string(), v.to_string()) }
            / c:name() _ ">" _ v:value() _? { Condition::Greater(c.to_string(), v.to_string()) }
            / c:name() _ "<=" _ v:value() _? { Condition::LessEq(c.to_string(), v.to_string()) }
            / c:name() _ "<" _ v:value() _? { Condition::Less(c.to_string(), v.to_string()) }
            / c:name() _ "!=" _ v:value() _? { Condition::Ne(c.to_string(), v.to_string()) }
            / c:name() _ ("BETWEEN" / "between") _ f:value() _ ("AND" / "and") _ t:value() _? {
                Condition::Between(c.to_string(), f.to_string(), t.to_string())
            }
        }
        / expected!("condition")

    rule operation() -> ()
        =  quiet!{"CREATE TABLE"
        / "create table"
        / "SELECT"
        / "select"
        / "INSERT INTO"
        / "insert into"
        / "UPDATE"
        / "update"
        / "DELETE FROM"
        / "delete from"
        / "DROP TABLE"
        / "drop table"
        / "ALTER TABLE"
        / "alter table"
        / "BEGIN TRANSACTION"
        / "begin transaction"
        / "COMMIT"
        / "commit"
        / "ROLLBACK"
        / "rollback"
        / "END TRANSACTION"
        / "end transaction"
        / "CREATE INDEX"
        / "create index"}
        / expected!("operation")

    rule column() -> (&'input str, CellType, bool, bool, bool)
        =  name:name() _ t:cell_type() _ pk:primary_key()? _ auto_inc:auto()? _ not_null:unnullable()? {
            let is_pk = pk.is_some();
            let is_nullable = if is_pk { false } else if not_null.is_some() { false } else { true };
            (name, t, is_pk, auto_inc.is_some(), is_nullable)
        }
        / expected!("column")

    rule primary_key() -> ()
        =  "PRIMARY KEY"
        / "primary key"
        / expected!("primary key")

    rule auto() -> ()
        =  "AUTOINCREMENT"
        / "autoincrement"
        / expected!("autoincrement")

    rule unnullable() -> ()
        =  quiet!{"NOT NULL"}
        / quiet!{"not null"}
        / expected!("nullable")

    rule columns() -> Vec<&'input str>
        =  "*" { vec!["*"] }
        / "count(*)" { vec!["count(*)"] }
        / columns:(name() ** ",") { columns }
        / expected!("columns")

    rule cell_type() -> CellType
        =  "integer" { CellType::Integer }
        / "float" { CellType::Float }
        / "text" { CellType::Text }
        / "blob" { CellType::Blob }
        / expected!("cell type")
  }
}

pub fn create_idx_sql(s: &str) -> Result<(String, String), anyhow::Error> {
    let create = sql::create_idx(s).or_else(|e| {
        Err(anyhow::anyhow!(
            "Failed to parse create index statement: {:?}",
            e
        ))
    })?;
    Ok(create)
}

pub fn create_sql(s: &str) -> Result<Table, anyhow::Error> {
    let create = sql::create(s)
        .or_else(|e| Err(anyhow::anyhow!("Failed to parse create statement: {:?}", e)))?;
    Ok(create)
}

#[cfg(test)]
fn assert_create(test: &str, expected: Table) {
    let answer = Table::from_str(test).unwrap();
    assert_eq!(answer, expected);
}

#[cfg(test)]
fn assert_select(test: &str, expected: Select) {
    let answer = Select::from_str(test).unwrap();
    assert_eq!(answer, expected);
}

#[test]
fn test_create() {
    assert_create(
        "create table \"apples\" 
        (
            id integer primary key autoincrement
        , name text not null, color text, \"some thing\" text)",
        Table {
            name: "apples".to_string(),
            columns: HashMap::from_iter(vec![
                (
                    "id".to_string(),
                    Column {
                        name: "id".to_string(),
                        cell_type: CellType::Integer,
                        idx: 0,
                        pk: true,
                        nullable: false,
                    },
                ),
                (
                    "name".to_string(),
                    Column {
                        name: "name".to_string(),
                        cell_type: CellType::Text,
                        idx: 1,
                        pk: false,
                        nullable: false,
                    },
                ),
                (
                    "color".to_string(),
                    Column {
                        name: "color".to_string(),
                        cell_type: CellType::Text,
                        idx: 2,
                        pk: false,
                        nullable: true,
                    },
                ),
                (
                    "some thing".to_string(),
                    Column {
                        name: "some thing".to_string(),
                        cell_type: CellType::Text,
                        idx: 3,
                        pk: false,
                        nullable: true,
                    },
                ),
            ]),
            key: Some("id".to_string()),
        },
    );
}

#[test]
fn test_select_all_lowercase() {
    assert_select(
        "select * from apples",
        Select {
            name: "apples".to_string(),
            columns: SelectColumns::All,
            conds: vec![],
        },
    );
}

#[test]
fn test_select_all_uppercase() {
    assert_select(
        "SELECT * FROM apples",
        Select {
            name: "apples".to_string(),
            columns: SelectColumns::All,
            conds: vec![],
        },
    );
}

#[test]
fn test_select_all_different_case() {
    assert_select(
        "select * FROM apples",
        Select {
            name: "apples".to_string(),
            columns: SelectColumns::All,
            conds: vec![],
        },
    );
}

#[test]
fn test_count_rows() {
    assert_select(
        "SELECT count(*) FROM apples",
        Select {
            name: "apples".to_string(),
            columns: SelectColumns::Count,
            conds: vec![],
        },
    );
}

#[test]
fn test_select_col() {
    assert_select(
        "SELECT name FROM apples",
        Select {
            name: "apples".to_string(),
            columns: SelectColumns::Columns(vec!["name".to_string()]),
            conds: vec![],
        },
    );
}

#[test]
fn test_select_cols_without_space_after_comma() {
    assert_select(
        "SELECT name,color FROM apples",
        Select {
            name: "apples".to_string(),
            columns: SelectColumns::Columns(vec!["name".to_string(), "color".to_string()]),
            conds: vec![],
        },
    );
}

#[test]
fn test_select_cols_with_space_after_comma() {
    assert_select(
        "SELECT name, color FROM apples",
        Select {
            name: "apples".to_string(),
            columns: SelectColumns::Columns(vec!["name".to_string(), "color".to_string()]),
            conds: vec![],
        },
    );
}

#[test]
fn test_select_where() {
    assert_select(
        "SELECT * FROM apples WHERE name = 'red'",
        Select {
            name: "apples".to_string(),
            columns: SelectColumns::All,
            conds: vec![Condition::Eq("name".to_string(), "red".to_string())],
        },
    );
}

#[test]
fn test_select_cols_where() {
    assert_select(
        "SELECT name, eye_color FROM people WHERE eye_color = 'Dark Red'",
        Select {
            name: "people".to_string(),
            columns: SelectColumns::Columns(vec!["name".to_string(), "eye_color".to_string()]),
            conds: vec![Condition::Eq(
                "eye_color".to_string(),
                "Dark Red".to_string(),
            )],
        },
    );
}

#[test]
fn test_select_where_and() {
    assert_select(
        "select * from apples where name = 'red' and id = 297",
        Select {
            name: "apples".to_string(),
            columns: SelectColumns::All,
            conds: vec![
                Condition::Eq("name".to_string(), "red".to_string()),
                Condition::Eq("id".to_string(), "297".to_string()),
            ],
        },
    );
}

#[test]
fn test_select_where_more_than() {
    assert_select(
        "select * from apples where name = 'red' and id > 297",
        Select {
            name: "apples".to_string(),
            columns: SelectColumns::All,
            conds: vec![
                Condition::Eq("name".to_string(), "red".to_string()),
                Condition::Greater("id".to_string(), "297".to_string()),
            ],
        },
    );
}
