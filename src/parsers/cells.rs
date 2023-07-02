use std::num::NonZeroU64;

use nom::IResult;
use nom::{combinator::into, multi::many1, number::complete::be_u16};

use crate::sqlite::pages::Page;

use super::{
    records::{RecordCode, Value},
    varint::varint,
};

#[derive(Clone, Copy, PartialEq)]
/// Contains the payload part of the [Cell].
pub struct Payload<'a> {
    pub size: u64,
    pub payload: &'a [u8],
    pub overflow: Option<u32>,
}

impl<'a> Payload<'a> {
    pub fn parse(&'a self) -> IResult<&'a [u8], Vec<Value>> {
        let (_, header_size) = varint(self.payload)?;
        let header = &self.payload[..header_size as usize];
        let (header, _) = varint(header)?; // We don't need the size which is the first varint
        let (_, codes): (_, Vec<RecordCode>) = many1(into(varint))(header)?;
        let mut body = &self.payload[header_size as usize..];
        let mut records = vec![];
        for code in codes {
            let (input, rec) = code.parse(body)?;
            body = input;
            records.push(rec);
        }
        Ok((body, records))
    }
}

impl<'a> std::fmt::Debug for Payload<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Payload")
            .field("size", &self.size)
            .field("payload", &String::from_utf8_lossy(&self.payload))
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
/// Represents a cell in a table or index.
pub enum Cell<'a> {
    /// Table Leaf cell
    TableLeaf { row_id: u64, payload: Payload<'a> },
    /// Table Interior cell
    TableInterior { left_child_page: u32, row_id: u64 },
    /// Index Leaf cell
    IndexLeaf { payload: Payload<'a> },
    /// Index Interior cell
    IndexInterior {
        left_child_page: u32,
        payload: Payload<'a>,
    },
}

impl<'a> Cell<'a> {
    pub fn next_page(&self) -> Option<NonZeroU64> {
        match self {
            Cell::TableLeaf { .. } => None,
            Cell::TableInterior {
                left_child_page, ..
            } => Some(NonZeroU64::new(*left_child_page as u64)?),
            Cell::IndexLeaf { .. } => None,
            Cell::IndexInterior {
                left_child_page, ..
            } => Some(NonZeroU64::new(*left_child_page as u64)?),
        }
    }
}

impl<'a> Cell<'a> {
    pub fn get_payload(&self) -> Option<&Payload<'a>> {
        match self {
            Cell::TableLeaf { ref payload, .. } => Some(payload),
            Cell::TableInterior { .. } => None,
            Cell::IndexLeaf { ref payload, .. } => Some(payload),
            Cell::IndexInterior { ref payload, .. } => Some(payload),
        }
    }
}

impl<'a> TryFrom<Cell<'a>> for Vec<Value> {
    type Error = anyhow::Error;
    fn try_from(value: Cell<'a>) -> Result<Self, Self::Error> {
        let pl = value
            .get_payload()
            .ok_or_else(|| anyhow::anyhow!("Table Interior cells have no payload"))?;
        let (_, mut row) = pl
            .parse()
            .map_err(|e| anyhow::anyhow!("Parse payload error: {}", e.to_string()))?;

        if let Cell::TableLeaf { row_id, .. } = value {
            match row[0] {
                Value::Null => row[0] = Value::Integer(row_id as i64),
                _ => {}
            }
        }
        Ok(row)
    }
}

#[derive(Debug, Clone)]
pub struct CellIter<'p> {
    pub page: &'p Page,
    pub ptr_array: &'p [u8],
}

impl<'p> Iterator for CellIter<'p> {
    type Item = Cell<'p>;
    fn next(&mut self) -> Option<Self::Item> {
        let (input, ptr) = be_u16::<&[u8], ()>(self.ptr_array).ok()?;
        let data = &self.page[ptr as usize..];
        let (_, cell) = self.page.header.parse_cell(data).ok()?;
        self.ptr_array = input;
        Some(cell)
    }
}
