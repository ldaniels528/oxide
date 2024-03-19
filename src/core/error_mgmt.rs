////////////////////////////////////////////////////////////////////
// error management module
////////////////////////////////////////////////////////////////////

use std::io;

#[macro_export]
macro_rules! cnv_error {
    ($e:expr) => {
        io::Error::new(io::ErrorKind::Other, $e)
    };
}

pub fn fail<A>(message: impl Into<String>) -> io::Result<A> {
    Err(io::Error::new(io::ErrorKind::Other, message.into()))
}