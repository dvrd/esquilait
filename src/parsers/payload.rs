use nom::{combinator::into, multi::many1, IResult};

use super::{record_code::RecordCode, value::Value, varint::varint};

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
