use std::ffi::CString;

#[repr(C)]
pub struct Context {

    current_plugin_dir: *const std::os::raw::c_char,

}

impl Context {


    pub fn new(current_plugin_dir: &str) -> Context {
        let c_str:CString = CString::new(current_plugin_dir).unwrap();
        Context {
            current_plugin_dir: c_str.into_raw(),
        }
    }

    pub fn get_current_plugin_dir(&self) -> &str {
        unsafe { 
            std::ffi::CStr::from_ptr(self.current_plugin_dir).to_str().unwrap()
        }
    }

}