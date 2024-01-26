use std::collections::HashMap;

use super::plugin::plugin_host::PluginHost;

pub struct PluginService {
    inner: HashMap<String, Vec<PluginHost>>
}

