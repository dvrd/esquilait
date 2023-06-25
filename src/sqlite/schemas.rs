use anyhow::{bail, Result};
use std::str::FromStr;

use super::Row;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SchemaType {
    Table,
    Index,
    View,
    Trigger,
}

impl FromStr for SchemaType {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use SchemaType::*;
        match s {
            "table" => Ok(Table),
            "index" => Ok(Index),
            "view" => Ok(View),
            "trigger" => Ok(Trigger),
            _ => bail!("schema type must be table, index, view or trigger"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub stype: SchemaType,
    pub name: String,
    pub table_name: String,
    pub rootpage: u64,
    pub sql: String,
}

impl Schema {
    pub fn new(row: Row) -> Result<Self> {
        let stype = row[0]
            .to_string()
            .parse()?;
        let name = row[1].to_string();
        let table_name = row[2].to_string();
        let rootpage = u64::from(row[3].clone());
        let sql = row[4].to_string();

        Ok(Self {
            stype,
            name,
            table_name,
            rootpage,
            sql,
        })
    }
}
