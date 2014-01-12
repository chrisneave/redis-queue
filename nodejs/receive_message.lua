-- KEYS[1] = KEY of the message that was received
-- KEYS[2] = unique SET containing all received messages
-- KEYS[3] = unique key value for the message
-- ARGV[1] = TIME value for received_at

local m_key = KEYS[1]
redis.call("HSET", m_key, "status", "received")
redis.call("HSET", m_key, "received_at", ARGV[1])
return redis.call("HGETALL", m_key)