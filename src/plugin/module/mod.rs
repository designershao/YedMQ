use mlua::{Lua, Table, UserData, FromLua};
use anyhow::Result;

use self::hook::Hook;

pub mod hook;

#[derive(FromLua, Clone)]
pub struct Samoye {
    pub hook: Hook
}

impl Samoye {
    pub fn new(lua:&Lua) -> Self {
        Samoye { hook: Hook::new(lua) }
    }
}

impl UserData for Samoye {
    fn add_methods<'lua, M: mlua::UserDataMethods<'lua, Self>>(methods: &mut M) {
        
    }
    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("Hook", |_, this| Ok(this.hook));
    }
}

pub fn init_runtime(lua: &Lua) -> Result<()> {
    lua.globals().set("Samoye", Samoye::new(lua))?;
    Ok(())
}

pub fn get_root_module(lua: &Lua) -> Result<Samoye> {
    let root_module:Samoye = lua.globals().get("Samoye")?;
    Ok(root_module)
}