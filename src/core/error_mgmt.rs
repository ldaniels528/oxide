////////////////////////////////////////////////////////////////////
// error management module
////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! cnv_error {
    ($e:expr) => {
        std::io::Error::new(std::io::ErrorKind::Other, $e)
    };
}

pub fn fail<A>(message: impl Into<String>) -> std::io::Result<A> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, message.into()))
}