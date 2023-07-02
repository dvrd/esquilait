use nom::{
    number::complete::{be_u16, be_u32, be_u8},
    IResult,
};

#[derive(Debug)]
pub struct DatabaseHeader {
    pub header_string: String,
    pub page_size: u16,
    pub file_format_write_version: u8,
    pub file_format_read_version: u8,
    pub reserved_space: u8,
    pub max_embedded_payload_fraction: u8,
    pub min_embedded_payload_fraction: u8,
    pub leaf_payload_fraction: u8,
    pub file_change_counter: u32,
    pub database_size: u32,
    pub first_freelist_trunk_page: u32,
    pub total_freelist_pages: u32,
    pub schema_cookie: u32,
    pub schema_format_number: u32,
    pub default_page_cache_size: u32,
    pub largest_root_btree_page_number: u32,
    pub database_text_encoding: u32,
    pub user_version: u32,
    pub incremental_vacuum_mode: u32,
    pub application_id: u32,
    pub version_valid_for: u32,
    pub sqlite_version_number: u32,
}

impl DatabaseHeader {
    pub fn new(input: &[u8]) -> IResult<&[u8], DatabaseHeader> {
        let header_string = match String::from_utf8(input[0..15].to_vec()) {
            Ok(s) => s,
            Err(_) => {
                return Err(nom::Err::Error(nom::error::Error {
                    input,
                    code: nom::error::ErrorKind::NoneOf,
                }))
            }
        };
        let buf = &input[16..]; // continue from page_size
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
}
