use mlua::Function;
use mlua::Result;

#[derive(Debug, PartialEq)]
pub struct OnConnectAuthHookFuncWrapper<'lua> {
    func: Function<'lua>,
    unique_name: String,
}

impl<'lua> Eq for OnConnectAuthHookFuncWrapper<'lua> {}

impl<'lua> std::hash::Hash for OnConnectAuthHookFuncWrapper<'lua> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.unique_name.hash(state)
    }
}

impl<'lua> OnConnectAuthHookFuncWrapper<'lua> {
    pub fn new(func: Function<'lua>, unique_name: String) -> Self {
        Self {
            func,
            unique_name
        }
    }

    pub fn call(&self, client_id: &String, username: &String, password: &String) -> Result<bool> {
        self.func.call((client_id.clone(), username.clone(), password.clone()))
    }
}
