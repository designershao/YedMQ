# Description
Plugin system is based lua script language.When system start, the plugin system scan the plugin directory and then load into seprate lua runtime instance.

# Design

## Plugin Folder Struct

```
plugin_a/
├─ doc/
├─ src/
│  ├─ common/
│  │  ├─ utils.lua
│  ├─ plugin.lua
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

```lua

local M =  {}

local function ConnectAuth(clientId, username, password, ip)
    return true
end

function M.OnActivate()
    samoye.Hooks.OnConnectAuth:Register(ConnectAuth)
end

function M.OnDeactivate()
end

return M
```
### OnAcativate 
This function will be called when the plugin load into system.

### OnDeactivate
This function will be called when the plugin unload from system.