plugin = {}
plugin.name = "demo_plugin"
plugin.version = "1.0.0"
plugin.author = "test"
plugin.description = "demo plugin"

hooks = {}

hooks.onConnectAuth = function(clientId, username, password, ip) 
    if(clientId == "client_id") 
    then
        return true
    else
        return false
    end
end

hooks.onPublish = function(sessionCtx, topic, qos, content)
    username = sessionCtx.username
    password = sessionCtx.clientId
    _G.onPublishPacketConentStr = content:readAsUtf8String()
    _G.onPublishPacketQos = qos
    _G.onPublishPacketTopic = topic
end

plugin.hook = hooks

return plugin