use super::{
    cells::{Cell, CellIter, Payload},
    varint::varint,
};
use anyhow::{bail, Error};
use nom::{
    bytes::complete::take,
    combinator::map_res,
    number::complete::{be_u16, be_u32, u8},
    sequence::tuple,
    IResult,
};
use std::ops::Deref;

#[derive(Debug, Clone)]
pub struct Page {
    pub page_id: u64,
    pub data: Vec<u8>,
    pub header: BtreeHeader,
}

impl Page {
    pub fn cells<'p>(&'p self) -> CellIter<'p> {
        let start = if self.page_id == 1 {
            108
        } else if self.header.kind.is_interior() {
            12
        } else {
            8
        };
        let count = self.header.cell_count as usize;
        let ptr_array = &self[start..count * 2 + start];

        CellIter {
            page: self,
            ptr_array,
        }
    }
}

impl Deref for Page {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageKind {
    IndexInterior,
    TableInterior,
    IndexLeaf,
    TableLeaf,
}

impl PageKind {
    pub fn is_interior(self) -> bool {
        match self {
            Self::IndexInterior | Self::TableInterior => true,
            _ => false,
        }
    }
}

impl TryFrom<u8> for PageKind {
    type Error = Error;
    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        use PageKind::*;
        match value {
            2 => Ok(IndexInterior),
            5 => Ok(TableInterior),
            10 => Ok(IndexLeaf),
            13 => Ok(TableLeaf),
            _ => bail!("invalid b-tree page type: {}", value),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BtreeHeader {
    /// Page type
    pub kind: PageKind,
    /// Offset to first freeblock in the page, or 0 if none.
    pub first_freeblock: u16,
    /// Number of cells on the page.
    pub cell_count: u16,
    /// Start of cell content area
    pub cell_contents: u16,
    /// Number of fragmented free bytes.
    pub fragmented_free_bytes: u8,
    /// Child page whose keys are greater than the keys on this page.
    /// Only exists if it's an internal page.
    pub rightmost_pointer: Option<u32>,
}

impl<'a> BtreeHeader {
    /// Parse a cell based on the type of Btree.
    pub fn parse_cell(&'a self, input: &'a [u8]) -> IResult<&[u8], Cell<'a>> {
        match self.kind {
            PageKind::TableLeaf => {
                let (input, (size, row_id)) = tuple((varint, varint))(input)?;
                let (input, payload) = take(size)(input)?;
                let payload = Payload {
                    size,
                    payload,
                    overflow: None,
                };
                Ok((input, Cell::TableLeaf { row_id, payload }))
            }

            PageKind::TableInterior => {
                let (input, (left_child_page, row_id)) = tuple((be_u32, varint))(input)?;
                Ok((
                    input,
                    Cell::TableInterior {
                        left_child_page,
                        row_id,
                    },
                ))
            }

            PageKind::IndexLeaf => {
                let (input, size) = varint(input)?;
                let (input, payload) = take(size)(input)?;
                let payload = Payload {
                    size,
                    payload,
                    overflow: None,
                };
                Ok((input, Cell::IndexLeaf { payload }))
            }

            PageKind::IndexInterior => {
                let (input, (left_child_page, size)) = tuple((be_u32, varint))(input)?;
                let (input, payload) = take(size)(input)?;
                let payload = Payload {
                    size,
                    payload,
                    overflow: None,
                };
                Ok((
                    input,
                    Cell::IndexInterior {
                        left_child_page,
                        payload,
                    },
                ))
            }
        }
    }
}

pub fn parse_btree_header(input: &[u8]) -> IResult<&[u8], BtreeHeader> {
    let (
        input,
        (
            kind,
            first_freeblock,
            cell_count,
            cell_contents,
            fragmented_free_bytes,
            rightmost_pointer,
        ),
    ) = tuple((
        map_res(u8, |n| PageKind::try_from(n)),
        be_u16,
        be_u16,
        be_u16,
        u8,
        be_u32,
    ))(input)?;
    let rightmost_pointer = kind.is_interior().then_some(rightmost_pointer);
    Ok((
        input,
        BtreeHeader {
            kind,
            first_freeblock,
            cell_count,
            cell_contents,
            fragmented_free_bytes,
            rightmost_pointer,
        },
    ))
}
