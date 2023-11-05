local M =  {}

local function ConnectAuth(clientId, username, password, ip)
    return true
end

function M.OnActivate()
    Samoye.Hook.OnConnectAuth:Register(ConnectAuth)
end

function M.OnDeactivate()
end

return M