use mlua::{FromLua, UserData};

// Represent current session context
#[derive(Clone,FromLua)]
pub struct SessionContext {
    pub tenant_id: String,
    pub client_identifier: String,
    pub username: String,
    pub remote_addr: String
}

impl UserData for SessionContext {
    fn add_fields<'lua, F: mlua::prelude::LuaUserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("tenantId",|_,this| Ok(this.tenant_id.clone()));
        fields.add_field_method_get("clientIdentifier",|_,this| Ok(this.client_identifier.clone()));
        fields.add_field_method_get("username",|_,this| Ok(this.username.clone()));
        fields.add_field_method_get("remoteAddr",|_,this| Ok(this.remote_addr.clone()));
    }

    fn add_methods<'lua, M: mlua::prelude::LuaUserDataMethods<'lua, Self>>(methods: &mut M) {
        
    }
}