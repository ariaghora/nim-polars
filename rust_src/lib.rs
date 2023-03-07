use polars::prelude::*;
use std::ffi::{c_char, CStr, CString};
use std::fmt::{Display, Formatter, Result};
use std::ptr::{self, drop_in_place};

pub struct RsSeries(Series);
impl Display for RsSeries {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)
    }
}
pub struct RsDataFrame(DataFrame);
impl Display for RsDataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)
    }
}

fn _read_csv(path: &str) -> PolarsResult<DataFrame> {
    CsvReader::from_path(path)?.finish()
}

#[no_mangle]
pub unsafe extern "C" fn rs_read_csv(path: *const i8) -> *mut RsDataFrame {
    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(p) => p,
        Err(_) => {
            println!("Invalid UTF-8 on path to CSV");
            return ptr::null_mut();
        }
    };

    match _read_csv(path_str) {
        Ok(df) => {
            let boxed = Box::new(RsDataFrame(df));
            Box::into_raw(boxed)
        }
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn rs_series_to_str(df: *mut Series) -> *mut c_char {
    let df = &*df;
    CString::new(df.to_string()).unwrap().into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn rs_dataframe_to_str(df: *mut DataFrame) -> *mut c_char {
    let df = &*df;

    CString::new(df.to_string()).unwrap().into_raw()
}

#[no_mangle]
#[allow(unused)]
pub unsafe extern "C" fn rs_free_dataframe(df: *mut DataFrame) {
    // Box::from(df);
    drop_in_place(df);
}
