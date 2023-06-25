pub(crate) mod cells;
pub(crate) mod pages;
pub(crate) mod parser;
pub(crate) mod records;
pub(crate) mod schemas;
pub(crate) mod tables;
pub(crate) mod varint;

use self::{
    cells::Cell, pages::PageKind, parser::Condition, records::Value, schemas::Schema,
    tables::Table, varint::varint,
};
use anyhow::{anyhow, Result};
use core::fmt;
use nom::{
    number::complete::{be_u16, be_u32, be_u8},
    IResult,
};
use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    num::NonZeroU64,
};

type Row = Vec<Value>;

#[derive(Debug)]
pub struct Search {
    pub pgno: NonZeroU64,
    pub key: Option<String>,
    pub indeces: Option<Vec<u64>>,
    pub schema: Schema,
    pub cond: Option<Condition>,
}

impl Search {
    pub fn next_page(&self, pgno: NonZeroU64) -> Self {
        Self {
            pgno,
            key: self.key.clone(),
            indeces: self.indeces.clone(),
            schema: self.schema.clone(),
            cond: self.cond.clone(),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct DatabaseHeader {
    header_string: String,
    page_size: u16,
    file_format_write_version: u8,
    file_format_read_version: u8,
    reserved_space: u8,
    max_embedded_payload_fraction: u8,
    min_embedded_payload_fraction: u8,
    leaf_payload_fraction: u8,
    file_change_counter: u32,
    database_size: u32,
    first_freelist_trunk_page: u32,
    total_freelist_pages: u32,
    schema_cookie: u32,
    schema_format_number: u32,
    default_page_cache_size: u32,
    largest_root_btree_page_number: u32,
    database_text_encoding: u32,
    user_version: u32,
    incremental_vacuum_mode: u32,
    application_id: u32,
    version_valid_for: u32,
    sqlite_version_number: u32,
}

#[derive(Debug)]
pub struct Database {
    file: RefCell<File>,
    header: DatabaseHeader,
    first_page: pages::Page,
}

pub fn parse_db_header(input: &[u8]) -> IResult<&[u8], DatabaseHeader> {
    let header_string = match String::from_utf8(input[0..15].to_vec()) {
        Ok(s) => s,
        Err(_) => {
            return Err(nom::Err::Error(nom::error::Error {
                input,
                code: nom::error::ErrorKind::NoneOf,
            }))
        }
    };
    let buf = &input[16..];
    let (buf, page_size) = be_u16(buf)?;
    let (buf, file_format_write_version) = be_u8(buf)?;
    let (buf, file_format_read_version) = be_u8(buf)?;
    let (buf, reserved_space) = be_u8(buf)?;
    let (buf, max_embedded_payload_fraction) = be_u8(buf)?;
    let (buf, min_embedded_payload_fraction) = be_u8(buf)?;
    let (buf, leaf_payload_fraction) = be_u8(buf)?;
    let (buf, file_change_counter) = be_u32(buf)?;
    let (buf, database_size) = be_u32(buf)?;
    let (buf, first_freelist_trunk_page) = be_u32(buf)?;
    let (buf, total_freelist_pages) = be_u32(buf)?;
    let (buf, schema_cookie) = be_u32(buf)?;
    let (buf, schema_format_number) = be_u32(buf)?;
    let (buf, default_page_cache_size) = be_u32(buf)?;
    let (buf, largest_root_btree_page_number) = be_u32(buf)?;
    let (buf, database_text_encoding) = be_u32(buf)?;
    let (buf, user_version) = be_u32(buf)?;
    let (buf, incremental_vacuum_mode) = be_u32(buf)?;
    let (buf, application_id) = be_u32(buf)?;

    let buf = &buf[20..]; // skip reserved space

    let (buf, version_valid_for) = be_u32(buf)?;
    let (_, sqlite_version_number) = be_u32(buf)?;
    Ok((
        input,
        DatabaseHeader {
            header_string,
            page_size,
            file_format_write_version,
            file_format_read_version,
            reserved_space,
            max_embedded_payload_fraction,
            min_embedded_payload_fraction,
            leaf_payload_fraction,
            file_change_counter,
            database_size,
            first_freelist_trunk_page,
            total_freelist_pages,
            schema_cookie,
            schema_format_number,
            default_page_cache_size,
            largest_root_btree_page_number,
            database_text_encoding,
            user_version,
            incremental_vacuum_mode,
            application_id,
            version_valid_for,
            sqlite_version_number,
        },
    ))
}

impl Database {
    pub fn new(mut file: File) -> Result<Self> {
        let mut buf = [0u8; 100];
        file.by_ref().read_exact(&mut buf)?;

        let (_, db_header) =
            parse_db_header(&buf).map_err(|_| anyhow!("parsing db header at Database::new"))?;

        file.seek(SeekFrom::Start(0))?;
        let mut data = vec![0u8; db_header.page_size as usize];
        file.by_ref().read_exact(&mut data)?;

        let (_, header) = pages::parse_btree_header(&data[100..])
            .map_err(|e| anyhow!("parsing btree header at Database::new:\n{}", e))?;

        Ok(Self {
            file: RefCell::new(file),
            header: db_header,
            first_page: pages::Page {
                page_id: 1,
                data,
                header,
            },
        })
    }

    pub fn get_page(&self, pgno: NonZeroU64) -> Result<pages::Page> {
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

        let (_, header) = pages::parse_btree_header(hdata)
            .map_err(|e| anyhow!("parsing header at Database::get_page:\n{}", e))
            .unwrap();

        Ok(pages::Page {
            page_id: pgno,
            data,
            header: header.clone(),
        })
    }

    pub fn rows(&self, search: Search) -> Vec<Row> {
        match self.get_page(search.pgno) {
            Ok(page) => match &page.header.kind {
                PageKind::TableInterior => {
                    let rightmost_pointer =
                        NonZeroU64::new(page.header.rightmost_pointer.unwrap().into()).unwrap();

                    let mut rows: Vec<Row> = page
                        .cells()
                        .flat_map(|cell| {
                            match cell.next_page().map(|pgno| search.next_page(pgno)) {
                                Some(next_search) => {
                                    if let Some(indeces) = &search.indeces {
                                        match cell {
                                            Cell::TableInterior { row_id, .. } => {
                                                if indeces.binary_search(&row_id).is_ok() {
                                                    return self.rows(next_search);
                                                }
                                            }
                                            _ => return vec![],
                                        }
                                    }
                                    self.rows(next_search)
                                }
                                None => vec![],
                            }
                        })
                        .collect();

                    rows.extend(self.rows(search.next_page(rightmost_pointer)));

                    rows
                }
                PageKind::IndexInterior => {
                    let mut indices = vec![];

                    let search_key = match search.key.clone() {
                        Some(key) => key,
                        None => return vec![],
                    };

                    let mut left_key: Option<String> = None;

                    for cell in page.cells() {
                        let row = TryInto::<Row>::try_into(cell).unwrap();
                        let row_key = row.first().unwrap().to_string();

                        left_key = match left_key {
                            Some(ref lk) => {
                                if lk <= &search_key && search_key <= row_key {
                                    let mut index_rows =
                                        match cell.next_page().map(|pgno| search.next_page(pgno)) {
                                            Some(next_search) => self.rows(next_search),
                                            None => continue,
                                        };

                                    if row_key == search_key {
                                        index_rows.push(row);
                                    }

                                    let pgno = NonZeroU64::new(search.schema.rootpage).unwrap();
                                    let row_indeces: Vec<u64> = index_rows
                                        .iter()
                                        .flat_map(|r| {
                                            let row_id: u64 = r.get(1)?.clone().into();
                                            Some(row_id)
                                        })
                                        .collect();

                                    if row_indeces.iter().sum::<u64>() == 0 {
                                        indices.extend(index_rows);
                                        break;
                                    }

                                    let rows = self.rows(Search {
                                        pgno,
                                        key: None,
                                        indeces: Some(row_indeces),
                                        schema: search.schema.clone(),
                                        cond: search.cond.clone(),
                                    });

                                    indices.extend(rows);
                                } else if row_key > search_key {
                                    break;
                                }
                                Some(row_key.clone())
                            }
                            None => {
                                if search_key <= row_key {
                                    match cell.next_page().map(|pgno| search.next_page(pgno)) {
                                        Some(next_search) => {
                                            let rows = self.rows(next_search);
                                            indices.extend(rows);
                                        }
                                        None => continue,
                                    };
                                }
                                Some(row_key.clone())
                            }
                        };
                    }

                    indices
                }
                PageKind::TableLeaf | PageKind::IndexLeaf => page
                    .cells()
                    .flat_map(|cell| {
                        let row = TryInto::<Row>::try_into(cell).ok()?;

                        if let Some(indeces) = &search.indeces {
                            if let Cell::TableLeaf { row_id, .. } = cell {
                                if !indeces.contains(&row_id) {
                                    return None;
                                }
                            }
                        }

                        if let Some(ref search_key) = search.key {
                            let row_key = row.first()?.to_string();
                            if row_key == search_key.to_string() {
                                return Some(row);
                            } else {
                                return None;
                            }
                        }

                        if let Some(cond) = &search.cond {
                            let table: Table = (&search.schema).try_into().ok()?;
                            if !cond.eval(&row, &table.columns) {
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

    pub fn get_schemas_vec(&self) -> Vec<schemas::Schema> {
        self.first_page
            .cells()
            .flat_map(|cell| match TryInto::<Row>::try_into(cell) {
                Ok(row) => schemas::Schema::new(row).ok(),
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

    pub fn get_schemas(&self) -> HashMap<String, schemas::Schema> {
        let mut schemas = HashMap::new();
        self.first_page
            .cells()
            .for_each(|cell| match TryInto::<Row>::try_into(cell) {
                Ok(row) => match schemas::Schema::new(row) {
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
