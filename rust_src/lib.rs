use polars::prelude::*;
use polars_lazy::prelude::*;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::ffi::{c_char, c_int, CStr, CString};
use std::fmt::{Display, Formatter, Result};
use std::ptr;

/// Following  structs are just a wrapper to implement custom traits
pub struct RsSeries {
    data: Series
}

impl Display for RsSeries {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.data.borrow().to_string())
    }
}

#[derive(Clone)]
pub struct RsDataFrame {
    data: RefCell<DataFrame>,
}

impl Display for RsDataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.data.borrow().to_string())
    }
}

#[derive(Clone)]
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
        Ok(df) => Box::into_raw(Box::new(RsDataFrame {
            data: RefCell::new(df),
        })),
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
    let boxed = Box::new(RsDataFrame {
        data: RefCell::new(res),
    });
    Box::into_raw(boxed)
}

#[no_mangle]
pub unsafe extern "C" fn read_csv(path: *const c_char) -> *mut RsDataFrame {
    let path_str = cstr_to_str(path);
    let reader = CsvReader::from_path(path_str);

    return match reader {
        Ok(r) => match r.finish() {
            Ok(df) => Box::into_raw(Box::new(RsDataFrame {
                data: RefCell::new(df),
            })),
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

#[no_mangle]
#[allow(unused)]
pub unsafe extern "C" fn free_dataframe(df: *mut RsDataFrame) {
    let df = Box::from_raw(df);
    drop(df.data.borrow_mut());
}

#[no_mangle]
#[allow(unused)]
pub unsafe extern "C" fn free_lazyframe(lf: *mut RsLazyFrame) {
    let x = Box::from_raw(lf).data;
}
