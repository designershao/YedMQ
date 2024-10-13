
#[macro_export]
macro_rules! register_plugin {
    ($app:ty, $constructor:path) => {
        #[no_mangle]
        pub extern "C" fn _plugin_register() -> *mut dyn $crate::plugin::Plugin {

            let constructor: fn() -> $app = $constructor;

            let object = constructor();
            let boxed: Box<dyn $crate::plugin::Plugin> = Box::new(object);
            Box::into_raw(boxed)
        }
    };
}
