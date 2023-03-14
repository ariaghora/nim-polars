use polars::prelude::*;
use polars_lazy::prelude::*;
use std::borrow::Borrow;
use std::ffi::{c_char, c_int, CStr, CString};
use std::fmt::{Display, Formatter, Result};
use std::ptr;

/// Following  structs are just a wrapper to implement custom traits
pub struct RsSeries {
    data: Series,
}

impl Display for RsSeries {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.data.borrow().to_string())
    }
}

#[derive(Clone)]
pub struct RsDataFrame {
    data: DataFrame,
}

impl Display for RsDataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.data.borrow().to_string())
    }
}

pub struct RsLazyFrame {
    data: LazyFrame,
}

impl Display for RsLazyFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", "<LazyFrame>")
    }
}

unsafe fn cstr_to_str<'a>(s: *const c_char) -> &'a str {
    CStr::from_ptr(s).to_str().unwrap()
}

#[no_mangle]
pub unsafe extern "C" fn collect(lf: *mut LazyFrame) -> *mut RsDataFrame {
    let lf = Box::from_raw(lf).clone();
    match lf.collect() {
        Ok(df) => Box::into_raw(Box::new(RsDataFrame { data: df })),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn columns(
    df: *mut RsDataFrame,
    names: *const *const c_char,
    len: c_int,
) -> *mut RsDataFrame {
    let df = &*(&*df).data.borrow();
    let names = unsafe { std::slice::from_raw_parts(names, c_int::try_into(len).unwrap()) };

    let rust_strings: Vec<String> = names
        .iter()
        .map(|&s| unsafe { CStr::from_ptr(s) })
        .map(|cs| cs.to_str().unwrap().to_string())
        .collect();

    let res = df.borrow().select(rust_strings).unwrap();
    let boxed = Box::new(RsDataFrame { data: res });
    Box::into_raw(boxed)
}

#[no_mangle]
pub unsafe extern "C" fn head(df: *mut RsDataFrame, length: usize) -> *mut RsDataFrame {
    let df = &*(&*df).data.borrow();
    Box::into_raw(Box::new(RsDataFrame {
        data: df.head(Some(length)),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn sort_by(
    df: *mut RsDataFrame,
    names: *const *const c_char,
    len_names: c_int,
    reverse: *const c_int,
    len_reverse: c_int,
) -> *mut RsDataFrame {
    let df = &*(&*df).data.borrow();

    let names = unsafe { std::slice::from_raw_parts(names, c_int::try_into(len_names).unwrap()) };
    let rust_strings: Vec<String> = names
        .iter()
        .map(|&s| unsafe { CStr::from_ptr(s) })
        .map(|cs| cs.to_str().unwrap().to_string())
        .collect();

    let reverse =
        unsafe { std::slice::from_raw_parts(reverse, c_int::try_into(len_reverse).unwrap()) };

    let rust_bool: Vec<bool> = reverse.iter().map(|i| *i != 0).collect();

    let sorted = df.sort(rust_strings, rust_bool).unwrap();
    Box::into_raw(Box::new(RsDataFrame { data: sorted }))
}

#[no_mangle]
pub unsafe extern "C" fn read_csv(path: *const c_char) -> *mut RsDataFrame {
    let path_str = cstr_to_str(path);
    let reader = CsvReader::from_path(path_str);

    return match reader {
        Ok(r) => match r.finish() {
            Ok(df) => Box::into_raw(Box::new(RsDataFrame { data: df })),
            Err(_) => ptr::null_mut(),
        },
        Err(_) => ptr::null_mut(),
    };
}

#[no_mangle]
pub unsafe extern "C" fn scan_csv(path: *const c_char) -> *mut RsLazyFrame {
    match LazyCsvReader::new(cstr_to_str(path)).finish() {
        Ok(lf) => Box::into_raw(Box::new(RsLazyFrame { data: lf })),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn series_to_str(s: *mut RsSeries) -> *mut c_char {
    let s = &*(&*s).data.borrow();
    CString::new(s.to_string()).unwrap().into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn dataframe_to_str(df: *mut RsDataFrame) -> *mut c_char {
    let df = &*(&*df).data.borrow();
    CString::new(df.to_string()).unwrap().into_raw()
}
