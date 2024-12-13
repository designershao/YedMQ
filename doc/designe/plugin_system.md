# Description
Plugin is based dynamic library.When system start, the plugin system scan the plugin directory and then load into the system.

# Design

## Plugin Folder Struct

```
plugin_a/
├─ doc/
├─ lib_plugin.so
├─ plugin.toml
├─ README.md

```

## Metadata
Plugin metadata and config defined in plugin.toml file.

example:
```Toml
[plugin]
name = "demo_plugin"
author = "yedmq"
description = "Just a demo plugin"
version = "1.0.0"
entry = "./lib_plugin.so"
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

```rust

pub struct ExamplePlugin {

}

impl ExamplePlugin {
    pub fn new() -> Self {
        ExamplePlugin {}
    }
}

impl yedmq_plugin::plugin::Plugin for ExamplePlugin {
    // implement the plugin trait
}


egister_plugin!(ExamplePlugin, ExamplePlugin::new); // register plugin to plugin system

```

## Plugin Trait
### on_activate
When the plugin loaded into system, this method will be called.

### on_deactivate
When the plugin unload from then system, this method will be called.

### connect_authenticate
When a new client connects to the broker and performs login verification, this method will be called.

### publish_authorizate
When a client publishes a message, and the system needs to check if it has the permission to publish, this method will be called.

### subscribe_authorizate
When a client subscribes to a topic, and the system needs to check if it has the permission to subscribe, this method will be called.

### on_publish
When a client publishes a message, this method will be called.


### on_disconnect
When a client disconnect, this method will be called.