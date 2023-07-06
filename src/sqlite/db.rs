use anyhow::{anyhow, Result};
use core::fmt;

use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    num::NonZeroU64,
};

use super::{
    db_header::DatabaseHeader,
    pages::{BtreeHeader, Page, PageKind},
    schemas::Schema,
    tables::Table,
};
use crate::parsers::{cells::Cell, sql::Condition, value::Value};

pub type Row = Vec<Value>;

#[derive(Debug)]
pub struct Search {
    pub pgno: NonZeroU64,
    pub key: Option<String>,
    pub indeces: Option<Vec<u64>>,
    pub schema: Schema,
    pub conds: Vec<Condition>,
}

impl Search {
    pub fn next_page(&self, pgno: NonZeroU64) -> Self {
        Self {
            pgno,
            key: self.key.clone(),
            indeces: self.indeces.clone(),
            schema: self.schema.clone(),
            conds: self.conds.clone(),
        }
    }
}

#[derive(Debug)]
pub struct Database {
    file: RefCell<File>,
    header: DatabaseHeader,
    first_page: Page,
}

impl Database {
    pub fn new(mut file: File) -> Result<Self> {
        let mut buf = [0u8; 100];
        file.by_ref().read_exact(&mut buf)?;

        let (_, db_header) =
            DatabaseHeader::new(&buf).map_err(|_| anyhow!("parsing db header at Database::new"))?;

        file.seek(SeekFrom::Start(0))?;
        let mut data = vec![0u8; db_header.page_size as usize];
        file.by_ref().read_exact(&mut data)?;

        let (_, header) = BtreeHeader::new(&data[100..])
            .map_err(|e| anyhow!("parsing btree header at Database::new:\n{}", e))?;

        Ok(Self {
            file: RefCell::new(file),
            header: db_header,
            first_page: Page {
                page_id: 1,
                data,
                header,
            },
        })
    }

    pub fn get_page(&self, pgno: NonZeroU64) -> Result<Page> {
        let pgno = pgno.get();
        let mut data = vec![0u8; self.header.page_size as usize];
        self.file
            .borrow_mut()
            .seek(SeekFrom::Start(
                ((pgno - 1) * self.header.page_size as u64) as u64,
            ))
            .unwrap();
        self.file.borrow_mut().read_exact(&mut data[..]).unwrap();

        let hdata = if pgno == 1 { &data[100..] } else { &data[..] };

        let (_, header) = BtreeHeader::new(hdata)
            .map_err(|e| anyhow!("parsing header at Database::get_page:\n{}", e))
            .unwrap();

        Ok(Page {
            page_id: pgno,
            data,
            header: header.clone(),
        })
    }

    pub fn rows(&self, search: Search) -> Vec<Row> {
        match self.get_page(search.pgno) {
            Ok(page) => match &page.header.kind {
                PageKind::TableInterior => {
                    let mut rows: Vec<Row> = vec![];

                    for cell in page.cells() {
                        let cell_rows = match cell.next_page().map(|pgno| search.next_page(pgno)) {
                            Some(next_search) => {
                                if let Some(indeces) = &search.indeces {
                                    if let Cell::TableInterior { row_id, .. } = cell {
                                        if indeces.binary_search(&row_id).is_ok() {
                                            return self.rows(next_search);
                                        }
                                    }
                                }
                                self.rows(next_search)
                            }
                            None => vec![],
                        };
                        rows.extend(cell_rows);
                    }

                    if let Some(rightmost_pointer) = page.header.rightmost_pointer {
                        if let Some(rightmost_pointer) = NonZeroU64::new(rightmost_pointer.into()) {
                            rows.extend(self.rows(search.next_page(rightmost_pointer)));
                        }
                    }

                    rows
                }
                PageKind::IndexInterior => {
                    let mut indices = vec![];
                    let mut rows = vec![];

                    let (search_key, pgno) =
                        match (search.key.clone(), NonZeroU64::new(search.schema.rootpage)) {
                            (Some(key), Some(pgno)) => (key, pgno),
                            _ => return vec![],
                        };

                    let mut left_key: Option<String> = None;

                    for cell in page.cells() {
                        let row = match TryInto::<Row>::try_into(cell) {
                            Ok(row) => row,
                            Err(_) => continue,
                        };
                        let row_key = match row.first() {
                            Some(row_key) => row_key.to_string(),
                            None => continue,
                        };

                        left_key = match left_key {
                            Some(ref lk) => {
                                if lk <= &search_key && search_key <= row_key {
                                    match cell.next_page().map(|pgno| search.next_page(pgno)) {
                                        Some(next_search) => {
                                            let mut index_rows = self.rows(next_search);

                                            if row_key == search_key {
                                                index_rows.push(row);
                                            }

                                            let row_indeces = index_rows
                                                .iter()
                                                .flat_map(|r| {
                                                    let row_id = r.get(1)?.clone().into();
                                                    Some(row_id)
                                                })
                                                .collect::<Vec<u64>>();

                                            indices.extend(row_indeces);
                                        }
                                        None => continue,
                                    };
                                }

                                if *lk == search_key {
                                    break;
                                }
                                Some(row_key.clone())
                            }
                            None => {
                                if search_key <= row_key {
                                    match cell.next_page().map(|pgno| search.next_page(pgno)) {
                                        Some(next_search) => {
                                            rows.extend(self.rows(next_search));
                                        }
                                        None => continue,
                                    };
                                }
                                Some(row_key.clone())
                            }
                        };
                    }

                    if indices.len() > 0 {
                        let indexed_rows = self.rows(Search {
                            pgno,
                            key: None,
                            indeces: Some(indices),
                            schema: search.schema.clone(),
                            conds: search.conds.clone(),
                        });

                        rows.extend(indexed_rows);
                    }

                    rows
                }
                PageKind::TableLeaf | PageKind::IndexLeaf => page
                    .cells()
                    .flat_map(|cell| {
                        let row = TryInto::<Row>::try_into(cell).ok()?;

                        // Check if it matches searched index
                        if let Some(indeces) = &search.indeces {
                            if let Cell::TableLeaf { row_id, .. } = cell {
                                if !indeces.contains(&row_id) {
                                    return None;
                                }
                            }
                        }

                        // Check if it matches index key
                        if let Some(ref search_key) = search.key {
                            let row_key = row.first()?.to_string();
                            if row_key == search_key.to_string() {
                                return Some(row);
                            } else {
                                return None;
                            }
                        }

                        // If there are conditions check that it passes at least 1
                        if search.conds.len() > 0 {
                            let match_cnt = search
                                .conds
                                .iter()
                                .filter(|cond| {
                                    (&search.schema)
                                        .try_into()
                                        .map(|table: Table| cond.eval(&row, &table.columns))
                                        .unwrap_or(false)
                                })
                                .count();
                            if match_cnt != search.conds.len() {
                                return None;
                            }
                        }

                        Some(row)
                    })
                    .collect(),
            },
            Err(e) => {
                println!("Error getting page: {}", e);
                vec![]
            }
        }
    }

    pub fn get_schemas_vec(&self) -> Vec<Schema> {
        self.first_page
            .cells()
            .flat_map(|cell| match TryInto::<Row>::try_into(cell) {
                Ok(row) => Schema::new(row).ok(),
                Err(e) => {
                    println!(
                        "Error converting cell to row: {} | Database::get_schemas_vec",
                        e
                    );
                    None
                }
            })
            .collect()
    }

    pub fn get_schemas(&self) -> HashMap<String, Schema> {
        let mut schemas = HashMap::new();
        self.first_page
            .cells()
            .for_each(|cell| match TryInto::<Row>::try_into(cell) {
                Ok(row) => match Schema::new(row) {
                    Ok(schema) => {
                        schemas.insert(schema.name.clone(), schema);
                    }
                    Err(e) => {
                        println!("Error creating schema: {} | Database::get_schemas", e);
                    }
                },
                Err(e) => {
                    println!(
                        "Error converting cell to row: {} | Database::get_schemas",
                        e
                    );
                }
            });
        schemas
    }
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "\
            database page size:  {}\n\
            write format:        {}\n\
            read format:         {}\n\
            reserved bytes:      {}\n\
            file change counter: {}\n\
            database page count: {}\n\
            freelist page count: {}\n\
            schema cookie:       {}\n\
            schema format:       {}\n\
            default cache size:  {}\n\
            autovacuum top root: {}\n\
            incremental vacuum:  {}\n\
            text encoding:       {}\n\
            user version:        {}\n\
            application id:      {}\n\
            software version:    {}\n\
            number of tables:    {}\n\
            number of indexes:   ?\n\
            number of triggers:  ?\n\
            number of views:     ?\n\
            schema size:         ?\n\
            data version:        ?\n\
            ",
            self.header.page_size,
            self.header.file_format_write_version,
            self.header.file_format_read_version,
            self.header.reserved_space,
            self.header.file_change_counter,
            self.header.database_size,
            self.header.total_freelist_pages,
            self.header.schema_cookie,
            self.header.schema_format_number,
            self.header.default_page_cache_size,
            self.header.largest_root_btree_page_number,
            self.header.incremental_vacuum_mode,
            match self.header.database_text_encoding {
                1 => "1 (utf8)",
                2 => "2 (utf16le)",
                3 => "3 (utf16be)",
                _ => "unknown",
            },
            self.header.user_version,
            self.header.application_id,
            self.header.sqlite_version_number,
            self.first_page.header.cell_count,
        )
    }
}
