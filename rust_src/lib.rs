use polars::prelude::*;
use std::alloc::{dealloc, Layout};
use std::ffi::{c_char, c_int, CStr, CString};
use std::fmt::{Display, Formatter, Result};
use std::ptr::{self, drop_in_place};

/// Following two structs are just a wrapper to implement custom
/// traits
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
pub unsafe extern "C" fn columns(
    df: *mut DataFrame,
    names: *const *const c_char,
    len: c_int,
) -> *mut RsDataFrame {
    let df = &*df;
    let names = unsafe { std::slice::from_raw_parts(names, c_int::try_into(len).unwrap()) };

    let rust_strings: Vec<String> = names
        .iter()
        .map(|&s| unsafe { CStr::from_ptr(s) })
        .map(|cs| cs.to_str().unwrap().to_string())
        .collect();

    let res = df.select(rust_strings).unwrap();
    let boxed = Box::new(RsDataFrame(res));
    Box::into_raw(boxed)
}

#[no_mangle]
pub unsafe extern "C" fn read_csv(path: *const i8) -> *mut RsDataFrame {
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
pub unsafe extern "C" fn series_to_str(df: *mut Series) -> *mut c_char {
    let df = &*df;
    CString::new(df.to_string()).unwrap().into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn dataframe_to_str(df: *mut DataFrame) -> *mut c_char {
    let df = &*df;

    CString::new(df.to_string()).unwrap().into_raw()
}

#[no_mangle]
#[allow(unused)]
pub unsafe extern "C" fn free_dataframe(df: *mut DataFrame) {
    drop_in_place(df);
    dealloc(df as *mut u8, Layout::new::<DataFrame>());
}
