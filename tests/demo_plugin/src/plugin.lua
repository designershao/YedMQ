local M =  {}

local function ConnectAuth(clientId, username, password, ip)
    response = Samoye.Hook.OnConnectAuth.response()
    response.pass = true
    response.userId = "123"
    response.tenantId = "t-123"
    return response
end

local function SubscribeAuth(sessionCtx, topic, qos)
    response = Samoye.Hook.OnSubscribeACLCheck.response()
    response.pass = true
    return response
end

function M.OnActivate()
    Samoye.Hook.OnConnectAuth:Register(ConnectAuth)
    Samoye.Hook.OnSubscribeACLCheck:Register(SubscribeAuth)
end

function M.OnDeactivate()
end

return M