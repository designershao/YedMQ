# Description
Plugin system is based lua script language.When system start, the plugin system scan the plugin directory and then load into seprate lua runtime instance.

# Design

## Metadata
Plugin export as a lua table.

Examples:

```lua
local M =  {}
M.author = "Samoye"
M.name = "TestPlugin"
M.description = "Just A Test Plugin"

M.setup = function()

end

return M
```

### Plugin Basic Info
| name  | type  | description  | 
|---|---|---|
| author | string  | plugin author name  | 
| name   | string  | plugin name  |
| description   | string  | plugin description |

### Plugin Setup Function
When plugin loaded succeed, the system would call the setup() function, the plugin could use setup function to init the plugin.

## Plugin Folder Struct

```
plugin_a/
├─ doc/
├─ lua/
│  ├─ common/
│  │  ├─ utils.lua
│  ├─ init.lua
├─ README.md

```

## Plugin Load Flow

## Plugin API Design

### samoye.api.net

### samoye.api.db

## Plugin Example
