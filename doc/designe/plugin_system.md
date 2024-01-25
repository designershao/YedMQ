# Description
Plugin system is based rune script language.When system start, the plugin system scan the plugin directory and then load into seprate rune vm instance.

# Design

## Plugin Folder Struct

```
plugin_a/
├─ doc/
├─ src/
│  ├─ common/
│  │  ├─ utils.rn
│  ├─ plugin.rn
├─ plugin.toml
├─ README.md

```

## Metadata
Plugin metadata and config defined in plugin.toml file.

example:
```Toml
[plugin]
name = "demo_plugin"
author = "Samoye"
description = "Just a demo plugin"
version = "1.0.0"
entry = "./src/plugin.lua"
priority = 1000

[custom_section]
# all your custom config
```

### plugin section

| name  | type  | description  | 
|---|---|---|
| author | string  | plugin author name  | 
| name   | string  | plugin name  |
| description   | string  | plugin description |
| version | string | plugin version |
| entry | string | plugin entry file |
| priority | number | plugin priority |

### custom section
The plugin could put settings (database connection, username and etc) in custom section.

## entry

Example:

```rune
fn on_auth_logic(client_info) {
    if client_info.identifier_id == "client_a" {
        true
    } else {
        false
    }
}

fn on_publish_logic(publish_message) {
}

fn on_activate(context) {
    context.hook.subscribe(Hook::OnConnectAuth, on_auth_logic);
    context.hook.subscribe(Hook::OnPublish, on_publish_logic);
}

fn on_deactivate() {

}

```
### on_activate 
This function will be called when the plugin load into system.        

### on_deactivate
This function will be called when the plugin unload from system.

## Reference
### PluginContext
Plugin context is provided as the first parameter to the on_activate function.
