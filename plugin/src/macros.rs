
#[macro_export]
macro_rules! register_plugin {
    ($app:ty, $constructor:path) => {

        #[no_mangle]
        pub extern "C" fn _plugin_register() -> $crate::plugin::RegisterPluginResult {

            let constructor: fn() -> std::result::Result<$app, anyhow::Error> = $constructor;

            let object = constructor();
            if object.is_err() {
                let null_plugin = $crate::plugin::NullPlugin{};
                let null_plugin_boxed: Box<dyn $crate::plugin::Plugin> = Box::new(null_plugin);
                let error_c_str = std::ffi::CString::new(object.err().unwrap().to_string()).unwrap();
                let result = $crate::plugin::RegisterPluginResult {
                    plugin: Box::into_raw(null_plugin_boxed),
                    error_code: -1,
                    error_msg:error_c_str.into_raw() 
                };
                result
            } else {
                let boxed: Box<dyn $crate::plugin::Plugin> = Box::new(object.unwrap());
                let plugin_pointer = Box::into_raw(boxed);
                $crate::plugin::RegisterPluginResult {
                    plugin: plugin_pointer,
                    error_code: 0,
                    error_msg: std::ptr::null_mut(),
                }
            }
        }
    };
}
