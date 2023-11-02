use mlua::{Lua, Function, UserData, Table, String, AnyUserData, FromLua};

use crate::protocol::v3::connect::ConnectPacket;

#[derive(Clone,FromLua)]
struct Hook {
    on_connect_auth_hook: OnConnectAuthHook,
    on_subscribe_acl_check_hook: OnSubscribeACLCheckHook,
}

impl Hook {
    pub fn new(lua: &Lua) -> Self {
        lua.globals().set("__HOOK_TABLE__", lua.create_table().unwrap()).unwrap();
        Hook { on_connect_auth_hook: OnConnectAuthHook {  }, on_subscribe_acl_check_hook: OnSubscribeACLCheckHook {  } }
    }
}

impl UserData for Hook {
    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("OnConnectAuth", |_, this| Ok(this.on_connect_auth_hook));
        fields.add_field_method_get("OnSubscribeACLCheck", |_, this| Ok(this.on_subscribe_acl_check_hook));
    }
}

#[derive(Clone,FromLua)]
struct LuaConnectPacket(ConnectPacket);

impl UserData for LuaConnectPacket {
    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("username",|_,this| Ok(this.0.payload.username.clone()));
        fields.add_field_method_get("password",|_,this| Ok(this.0.payload.password.clone()));
        fields.add_field_method_get("clientIdentifier",|_,this| Ok(this.0.payload.client_identifier.clone()));
    }
    fn add_methods<'lua, M: mlua::UserDataMethods<'lua, Self>>(methods: &mut M) {
    }
}

#[derive(Clone, Copy)]
pub struct OnConnectAuthHook {
}

impl OnConnectAuthHook {
    pub fn handle(&self, lua: &Lua, connect_packet: &ConnectPacket) -> anyhow::Result<bool> {
        let hook_table:Table = lua.globals().get("__HOOK_TABLE__")?;
        let hook_func:Function = hook_table.get("OnConnectAuth")?;
        let r = hook_func.call::<_, bool>(Some(LuaConnectPacket(connect_packet.clone())))?;
        Ok(r)
    }
}

impl<'a> UserData for OnConnectAuthHook {
    fn add_methods<'lua, M: mlua::UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method_mut("Register", |lua ,this, hook_func:Function| {
            let hook_table:Table = lua.globals().get("__HOOK_TABLE__")?;
            hook_table.set("OnConnectAuth", hook_func).unwrap();
            Ok(())
        });
        methods.add_method("Handle", |lua, this, connect_packet:LuaConnectPacket| {
            let hook_table:Table = lua.globals().get("__HOOK_TABLE__")?;
            let hook_func:Function = hook_table.get("OnConnectAuth")?;
            let r = hook_func.call::<_, bool>(Some(connect_packet))?;
            Ok(r)
        });
    }
}

#[derive(Clone, Copy)]
struct OnSubscribeACLCheckHook {
}
impl<'a> UserData for OnSubscribeACLCheckHook {
    fn add_methods<'lua, M: mlua::UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method_mut("Register", |lua ,this, hook_func:Function| {
            let hook_table:Table = lua.globals().get("__HOOK_TABLE__")?;
            hook_table.set("OnSubscribeACLCheck", hook_func).unwrap();
            Ok(())
        });
    }

    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
    }
}

#[cfg(test)]
mod tests {
    use mlua::chunk;

    use crate::protocol::{v3::{connect::{VariableHeader, Payload}, fixed_header::FixHeader}, PacketType};

    use super::*;

    #[test]
    pub fn test_hook_in_runtime() {
        let lua = Lua::new();
        let hook = Hook::new(&lua);

        lua.globals().set("Hook", hook).unwrap();
        lua.load(r#"
            Hook.OnConnectAuth:Register(function ()
                print("hello")
            end)
        "#).exec().unwrap();
    }

    #[test]
    pub fn test_on_connect_auth_hook() {

        let variable_header = VariableHeader {
                protocol_name: "MQTT".to_string(),
                protocol_level: 0x04,
                username_flag: true,
                password_flag: true,
                will_retain: true,
                will_qos: 1,
                will_flag: true,
                clean_session: true,
                keep_alive: 0,
            };
        let payload = Payload {
            client_identifier: "MQTT".to_string(),
            will_topic: Some("MQTT".to_string()),
            will_message: Some("MQTT".to_string()),
            username: Some("admin".to_string()),
            password: Some("MQTT".to_string()),
        };

        let fix_header = FixHeader{
                packet_type: PacketType::CONNECT,
                qos: None,
                retain: None,
                dup: None,
                remaining_length: variable_header.get_length() + payload.get_length(),
            };

        let connect_packet = ConnectPacket {
            fix_header,
            variable_header,
            payload
        };



        let lua = Lua::new();
        let hook = Hook::new(&lua);

        lua.globals().set("Hook", hook).unwrap();
        lua.load(r#"
            Hook.OnConnectAuth:Register(function (req)
                if req.username == "admin" then
                    return true
                else 
                    return false
                end
            end)
        "#).exec().unwrap();
        let hook:Hook = lua.globals().get("Hook").unwrap();
        let r = hook.on_connect_auth_hook.handle(&lua, &connect_packet).unwrap();
        assert!(r);

    }
}