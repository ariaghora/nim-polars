use polars::prelude::*;
use polars_lazy::prelude::*;
use std::alloc::{dealloc, Layout};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::ffi::{c_char, c_int, CStr, CString};
use std::fmt::{Display, Formatter, Result};
use std::ptr;

/// Following  structs are just a wrapper to implement custom traits
pub struct RsSeries(Series);
impl Display for RsSeries {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone)]
pub struct RsDataFrame {
    df: RefCell<DataFrame>,
}
impl Display for RsDataFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.df.borrow())
    }
}

pub struct RsLazyFrame(LazyFrame);
impl Display for RsLazyFrame {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", "<LazyFrame>")
    }
}

unsafe fn cstr_to_str<'a>(s: *const i8) -> &'a str {
    CStr::from_ptr(s).to_str().unwrap()
}

#[no_mangle]
pub unsafe extern "C" fn collect(lf: *mut LazyFrame) -> *mut RsDataFrame {
    let lf = Box::from_raw(lf).clone();
    match lf.collect() {
        Ok(df) => Box::into_raw(Box::new(RsDataFrame {
            df: RefCell::new(df),
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
    let df = &*(&*df).df.borrow();
    let names = unsafe { std::slice::from_raw_parts(names, c_int::try_into(len).unwrap()) };

    let rust_strings: Vec<String> = names
        .iter()
        .map(|&s| unsafe { CStr::from_ptr(s) })
        .map(|cs| cs.to_str().unwrap().to_string())
        .collect();

    let res = df.borrow().select(rust_strings).unwrap();
    let boxed = Box::new(RsDataFrame {
        df: RefCell::new(res),
    });
    Box::into_raw(boxed)
}

#[no_mangle]
pub unsafe extern "C" fn read_csv(path: *const i8) -> *mut RsDataFrame {
    let path_str = cstr_to_str(path);

    let reader = CsvReader::from_path(path_str);
    return match reader {
        Ok(r) => match r.finish() {
            Ok(df) => Box::into_raw(Box::new(RsDataFrame {
                df: RefCell::new(df),
            })),
            Err(_) => ptr::null_mut(),
        },
        Err(_) => ptr::null_mut(),
    };
}

#[no_mangle]
pub unsafe extern "C" fn scan_csv(path: *const i8) -> *mut RsLazyFrame {
    match LazyCsvReader::new(cstr_to_str(path)).finish() {
        Ok(lf) => {
            return Box::into_raw(Box::new(RsLazyFrame(lf)));
        }
        Err(_) => return ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn series_to_str(s: *mut Series) -> *mut c_char {
    let df = &*s;
    CString::new(df.to_string()).unwrap().into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn dataframe_to_str(df: *mut RsDataFrame) -> *mut c_char {
    let df = &*(&*df).df.borrow();
    CString::new(df.to_string()).unwrap().into_raw()
    // CString::new("ASDASD").unwrap().into_raw()
}

#[no_mangle]
pub unsafe extern "C" fn free_dataframe(df: *mut RsDataFrame) {
    dealloc(df as *mut u8, Layout::new::<RsDataFrame>());
}

#[no_mangle]
pub unsafe extern "C" fn free_lazyframe(lf: *mut RsLazyFrame) {
    dealloc(lf as *mut u8, Layout::new::<RsLazyFrame>());
}
