use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Blob(Vec<u8>),
    Text(String),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, ""),
            Value::Integer(n) => write!(f, "{}", *n),
            Value::Float(n) => write!(f, "{}", *n),
            Value::Blob(b) => write!(f, "{:?}", b),
            Value::Text(s) => write!(f, "{}", s),
        }
    }
}

macro_rules! impl_from_value {
    ($($t:ty),* $(,)?) => {
        $(
            impl From<Value> for $t {
                fn from(v: Value) -> $t {
                    match v {
                        Value::Null => Default::default(),
                        Value::Integer(n) => n as $t,
                        Value::Float(n) => n as $t,
                        Value::Blob(_) => Default::default(),
                        Value::Text(s) => s.parse::<$t>().unwrap_or_default()
                    }
                }
            }
        )*
    }
}

impl_from_value!(u8, i8, u16, i16, u32, i32, u64, i64, f32, f64, usize);
