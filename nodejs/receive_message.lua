-- KEYS[1] = KEY of the message that was received
-- ARGV[1] = TIME value for received_at

local m_key = KEYS[1]
redis.call("HSET", m_key, "status", "received")
redis.call("HSET", m_key, "received_at", ARGV[1])
local m_body = redis.call("HGET", m_key, "body")
return m_body