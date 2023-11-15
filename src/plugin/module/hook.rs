use mlua::{Lua, Function, UserData, Table, String as LuaString, AnyUserData, FromLua, MetaMethod};

use crate::{protocol::v3::{connect::ConnectPacket, subscribe::SubscribePacket, publish::PublishPacket}, plugin::{session_context::SessionContext, plugin::ConnectInfo}};

#[derive(Default, Clone, FromLua)]
pub struct SubscribeACLResponse {
    pub pass: bool,
    pub reason: String,
}

impl UserData for SubscribeACLResponse {
    fn add_fields<'lua, F: mlua::prelude::LuaUserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("pass", |_,this| Ok(this.pass));
        fields.add_field_method_set("pass", |_, this, val| {
            this.pass = val;
            Ok(())
        });

        fields.add_field_method_get("reason", |_, this| Ok(this.reason.clone()));
        fields.add_field_method_set("reason", |_,this,val| {
            this.reason = val;
            Ok(())
        });
    }

    fn add_methods<'lua, M: mlua::prelude::LuaUserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_function(MetaMethod::Call,|_, ()| Ok(SubscribeACLResponse::default()));
    }
}

#[derive(Default, Clone,FromLua)]
pub struct ConnectAuthResponse {
    pub pass: bool,
    pub reason: String,
    pub user_id: String,
    pub tenant_id: String
}

impl UserData for ConnectAuthResponse {
    fn add_fields<'lua, F: mlua::prelude::LuaUserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("pass", |_,this| Ok(this.pass));
        fields.add_field_method_set("pass", |_, this, val| {
            this.pass = val;
            Ok(())
        });

        fields.add_field_method_get("reason", |_, this| Ok(this.reason.clone()));
        fields.add_field_method_set("reason", |_,this,val| {
            this.reason = val;
            Ok(())
        });

        fields.add_field_method_get("userId", |_, this| Ok(this.user_id.clone()));
        fields.add_field_method_set("userId", |_,this,val| {
            this.user_id = val;
            Ok(())
        });


        fields.add_field_method_get("tenantId", |_, this| Ok(this.tenant_id.clone()));
        fields.add_field_method_set("tenantId", |_,this,val| {
            this.tenant_id = val;
            Ok(())
        });
    }

    fn add_methods<'lua, M: mlua::prelude::LuaUserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_meta_function(MetaMethod::Call,|_, ()| Ok(ConnectAuthResponse::default()));
    }
}


#[derive(Clone,FromLua,Copy)]
pub struct Hook {
    pub on_connect_auth_hook: OnConnectAuthHook,
    pub on_subscribe_acl_check_hook: OnSubscribeACLCheckHook,
    pub on_publish_hook: OnPublishHook
}

impl Hook {
    pub fn new(lua: &Lua) -> Self {
        lua.globals().set("__HOOK_TABLE__", lua.create_table().unwrap()).unwrap();
        Hook {
             on_connect_auth_hook: OnConnectAuthHook {  }, 
             on_subscribe_acl_check_hook: OnSubscribeACLCheckHook {  } ,
             on_publish_hook: OnPublishHook {  }
        }
    }

    pub fn get_register_hooks(&self, lua: &Lua) -> Vec<&str> {
        let mut result = vec![];
        let hook_table:mlua::Table = lua.globals().get("__HOOK_TABLE__",).unwrap();
        if hook_table.contains_key("OnConnectAuth").unwrap() {
            result.push("OnConnectAuth");
        }
        if hook_table.contains_key("OnSubscribeACLCheck").unwrap() {
            result.push("OnSubscribeACLCheck");
        }
        if hook_table.contains_key("OnPublish").unwrap() {
            result.push("OnPublish");
        }

        result

    }
}

impl UserData for Hook {
    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("OnConnectAuth", |_, this| Ok(this.on_connect_auth_hook));
        fields.add_field_method_get("OnSubscribeACLCheck", |_, this| Ok(this.on_subscribe_acl_check_hook));
    }
}

#[derive(Clone,FromLua)]
struct LuaConnectPacket {
    packet: ConnectPacket,
    remote_addr: String
}

impl UserData for LuaConnectPacket {
    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("username",|_,this| Ok(this.packet.payload.username.clone()));
        fields.add_field_method_get("password",|_,this| Ok(this.packet.payload.password.clone()));
        fields.add_field_method_get("clientIdentifier",|_,this| Ok(this.packet.payload.client_identifier.clone()));
        fields.add_field_method_get("remoteAddr",|_,this| Ok(this.packet.payload.client_identifier.clone()));
    }
    fn add_methods<'lua, M: mlua::UserDataMethods<'lua, Self>>(methods: &mut M) {
    }
}

#[derive(Clone, FromLua)]
struct LuaPublishPacket(PublishPacket);
impl UserData for LuaPublishPacket {
    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("qos", |_, this| Ok(this.0.fix_header.qos));
        fields.add_field_method_get("topic", |_, this| Ok(this.0.variable_header.topic_name.clone()));
        fields.add_field_method_get("payload", |_, this| Ok(this.0.payload.payload.clone()));
    }

    fn add_methods<'lua, M: mlua::UserDataMethods<'lua, Self>>(methods: &mut M) {
    }
}

#[derive(Clone, Copy)]
pub struct OnConnectAuthHook {
}

impl OnConnectAuthHook {
    pub fn handle(&self, lua: &Lua, connect_info: &ConnectInfo) -> anyhow::Result<ConnectAuthResponse> {
        let hook_table:Table = lua.globals().get("__HOOK_TABLE__")?;
        let hook_func:Function = hook_table.get("OnConnectAuth")?;
        let lua_connect_packet = LuaConnectPacket {
            packet: connect_info.connect_packet.clone(),
            remote_addr: connect_info.remote_addr.clone()
        };
        let r = hook_func.call::<_, ConnectAuthResponse>(Some(lua_connect_packet))?;
        Ok(r)
    }
}

impl<'a> UserData for OnConnectAuthHook {
    fn add_fields<'lua, F: mlua::prelude::LuaUserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("response", |_, _| Ok(ConnectAuthResponse::default()));
    }

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
pub struct OnSubscribeACLCheckHook {
}

impl OnSubscribeACLCheckHook {
    pub fn handle(&self, lua: &Lua, session_ctx: &SessionContext, topic: String, qos: i32) -> anyhow::Result<SubscribeACLResponse> {
        let hook_table:Table = lua.globals().get("__HOOK_TABLE__")?;
        let hook_func:Function = hook_table.get("OnSubscribeACLCheck")?;
        let r = hook_func.call::<_, SubscribeACLResponse>((session_ctx.clone(), topic, qos))?;
        Ok(r)
    }
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
        fields.add_field_method_get("response", |_, _| Ok(SubscribeACLResponse::default()));
    }
}

#[derive(Clone, Copy)]
pub struct OnPublishHook {
}

impl OnPublishHook {
    pub fn handle(&self, lua: &Lua, publish_packet: &PublishPacket) -> anyhow::Result<()> {
        let hook_table:Table = lua.globals().get("__HOOK_TABLE__")?;
        let hook_func:Function = hook_table.get("OnPublish")?;
        hook_func.call::<_, _>(Some(LuaPublishPacket(publish_packet.clone())))?;
        Ok(())
    }
}

impl<'a> UserData for OnPublishHook {

    fn add_methods<'lua, M: mlua::UserDataMethods<'lua, Self>>(methods: &mut M) {
        methods.add_method_mut("Register", |lua ,this, hook_func:Function| {
            let hook_table:Table = lua.globals().get("__HOOK_TABLE__")?;
            hook_table.set("OnPublish", hook_func).unwrap();
            Ok(())
        });
        methods.add_method("Handle", |lua, this, publish_packet:LuaPublishPacket| {
            let hook_table:Table = lua.globals().get("__HOOK_TABLE__")?;
            let hook_func:Function = hook_table.get("OnPublish")?;
            hook_func.call::<_, bool>(Some(publish_packet))?;
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
                connectResponse = Hook.OnConnectAuth.response()
                if req.username == "admin" then
                    connectResponse.pass = true
                    connectResponse.userId = "admin"
                    connectResponse.tenantId = "test"
                else 
                    connectResponse.pass = false
                    connectResponse.reason = "not admin"
                end
                return connectResponse
            end)
        "#).exec().unwrap();
        let hook:Hook = lua.globals().get("Hook").unwrap();
        let connect_info = ConnectInfo {
            remote_addr: "127.0.0.1".to_string(),
            connect_packet
        };
        let r = hook.on_connect_auth_hook.handle(&lua, &connect_info).unwrap();
        assert_eq!(r.pass, true);

    }

    #[test]
    pub fn test_on_subscribe_acl_check_hook() {
        let lua = Lua::new();
        let hook = Hook::new(&lua);

        lua.globals().set("Hook", hook).unwrap();
        lua.load(r#"
            Hook.OnSubscribeACLCheck:Register(function (ctx, topic, qos)
                response = Hook.OnSubscribeACLCheck.response()
                if topic == "/a" and qos == 0 then
                    response.pass = true
                    response.reason = "pass"
                else 
                    response.pass = false
                    response.reason = "not pass"
                end
                return response
            end)
        "#).exec().unwrap();
        let session_ctx = SessionContext { tenant_id: "test".to_string(), client_identifier: "MQTT".to_string(), username: "admin".to_string(), remote_addr: "127.0.0.1".to_string() };

        let hook:Hook = lua.globals().get("Hook").unwrap();
        let r = hook.on_subscribe_acl_check_hook.handle(&lua, &session_ctx, "/a".to_string(), 0).unwrap();
        assert_eq!(r.pass, true);
    }

    #[test]
    pub fn test_on_get_register_hooks() {
        let lua = Lua::new();
        let hook = Hook::new(&lua);

        lua.globals().set("Hook", hook).unwrap();
        lua.load(r#"
            Hook.OnSubscribeACLCheck:Register(function (topic, qos)
                if topic == "/a" and qos == 0 then
                    return true
                else 
                    return false
                end
            end)
        "#).exec().unwrap();

        let hook:Hook = lua.globals().get("Hook").unwrap();
        let r = hook.get_register_hooks(&lua);
        assert_eq!("OnSubscribeACLCheck", r[0]);
    }

    #[test]
    pub fn test_on_connection_auth_response() {
        let lua = Lua::new();
        let hook = Hook::new(&lua);

        lua.globals().set("Hook", hook).unwrap();
        let r = lua.load(r#"
            r = Hook.OnConnectAuth.response()
            r.userId = "admin"
            r.tenantId = "test"
            r.pass = true
            return r
        "#).eval::<ConnectAuthResponse>().unwrap();
        assert_eq!("admin", r.user_id);
        assert_eq!("test", r.tenant_id);
    }
}