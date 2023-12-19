local M =  {}

local function ConnectAuth(connectInfo)
    response = Samoye.Hook.OnConnectAuth.response()
    response.pass = true
    response.userId = connectInfo.clientIdentifier
    response.tenantId = "t-123"
    return response
end

local function SubscribeAuth(sessionCtx, topic, qos)
    response = Samoye.Hook.OnSubscribeACLCheck.response()
    response.pass = true
    return response
end

local function PublishProcess(sessionCtx, packet)
end

function M.OnActivate()
    Samoye.Hook.OnConnectAuth:Register(ConnectAuth)
    Samoye.Hook.OnSubscribeACLCheck:Register(SubscribeAuth)
end

function M.OnDeactivate()
end

return M