use mlua::{UserData, Table, Function, Lua};
use bytes::{BufMut, BytesMut, Buf, Bytes};
use nom::AsBytes;
use mlua::Error;

// simple byte array module
struct LuaByteArray {
    inner: BytesMut,
}

impl LuaByteArray {

    // register to the lua runtime as an inner module
    pub fn register_to_lua_runtime(lua: &mlua::Lua) {
        lua.globals().set("ByteArray", lua.create_proxy::<LuaByteArray>().unwrap()).unwrap();
    }
}

impl UserData for LuaByteArray {
    fn add_methods<'lua, M: mlua::UserDataMethods<'lua, Self>>(methods: &mut M) {

        methods.add_function("init", |_, bytes:Table| {
            let mut bytes_vec:Vec<u8> = Vec::new(); 

            for pair in bytes.pairs::<String, u8>() {
                bytes_vec.push(pair.unwrap().1);
            }

            let byte_array = LuaByteArray {
                inner: BytesMut::from(bytes_vec.as_bytes())
            };
            Ok(byte_array)
        });

        methods.add_method_mut("readUnsignedByte", |_, this, ()| {
            let byte = this.inner.get_u8();
            Ok(byte)
        });

        methods.add_method_mut("readByte", |_, this, ()| {
            let byte = this.inner.get_i8(); 
            Ok(byte)
        });


        methods.add_method_mut("readInt", |_,this,()| {
            let int_val = this.inner.get_i32();
            Ok(int_val)
        });

        methods.add_method_mut("readFloat", |_, this, ()| {
            Ok(this.inner.get_f32())
        });

        methods.add_method_mut("readDouble", |_, this, ()| {
            Ok(this.inner.get_f64())
        });

        methods.add_method_mut("readAsUtf8String", |_, this, ()| {
            match std::str::from_utf8(this.inner.as_bytes()) {
                Ok(str) => {
                    return Ok(str.to_string());
                }
                Err(e) => Err(Error::external(e))
            }
        });


    }
}

#[cfg(test)]
mod tests {
    use super::LuaByteArray;

    #[test]
    fn byte_array_init_test() {
        let lua = mlua::Lua::new();
        LuaByteArray::register_to_lua_runtime(&lua);
        let out = lua.load(r#"
            local a = ByteArray.init({0x68,0x65,0x6c,0x6c,0x6f})
            return a:readAsUtf8String()
        "#
        ).eval::<String>();
        assert_eq!(out.unwrap(), "hello");
    }

    #[test]
    fn byte_array_read_byte_test() {
        let lua = mlua::Lua::new();
        LuaByteArray::register_to_lua_runtime(&lua);
        let out = lua.load(r#"
            local a = ByteArray.init({0x68,0x65,0x6c,0x6c,0x6f})
            return a:readUnsignedByte()
        "#
        ).eval::<u8>();
        assert_eq!(out.unwrap(), 0x68);
    }

    #[test]
    fn byte_array_read_int_test() {
        let lua = mlua::Lua::new();
        LuaByteArray::register_to_lua_runtime(&lua);
        let out = lua.load(r#"
            local a = ByteArray.init({0x68,0x65,0x6c,0x6c,0x6f})
            return a:readInt()
        "#
        ).eval::<i32>();
        assert_eq!(out.unwrap(), 1751477356);
    }


}