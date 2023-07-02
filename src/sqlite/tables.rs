use anyhow::Error;
use itertools::Itertools;
use std::{collections::HashMap, str::FromStr};

use crate::parsers::sql::{create_sql, Select, SelectColumns};

use super::schemas::Schema;

#[derive(Debug, PartialEq, Clone, Copy, PartialOrd, Eq, Ord)]
pub enum CellType {
    Null,
    Integer,
    Float,
    Text,
    Blob,
}

impl std::str::FromStr for CellType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let t = match s {
            "integer" => CellType::Integer,
            "float" => CellType::Float,
            "text" => CellType::Text,
            "blob" => CellType::Blob,
            _ => CellType::Null,
        };
        Ok(t)
    }
}

#[derive(Debug, PartialEq, Clone, PartialOrd, Eq, Ord)]
pub struct Column {
    pub idx: usize,
    pub name: String,
    pub cell_type: CellType,
    pub nullable: bool,
    pub pk: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Table {
    pub name: String,
    pub columns: HashMap<String, Column>,
    pub key: Option<String>,
}

impl Table {
    /// Get [`Column`]s corresponding in a [`Select`]
    pub fn select(&self, sel: &Select) -> Vec<Column> {
        match &sel.columns {
            SelectColumns::Columns(cols) => cols
                .iter()
                .flat_map(|sc| self.columns.get(sc).cloned())
                .collect(),
            SelectColumns::Count => Vec::new(),
            SelectColumns::All => self.columns.values().sorted().cloned().collect(),
        }
    }
}

impl TryFrom<&Schema> for Table {
    type Error = Error;
    fn try_from(value: &Schema) -> std::result::Result<Self, Self::Error> {
        value.sql.parse()
    }
}

impl FromStr for Table {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        create_sql(s)
    }
}
