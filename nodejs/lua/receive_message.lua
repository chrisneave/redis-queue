-- KEYS[1] = KEY of submit queue
-- KEYS[2] = KEY of receive queue
-- KEYS[3] = KEY of the message already received in a blocking read
-- ARGV[1] = TIME value for received_at

local m_id = KEYS[3]

if m_id == nil then
  m_id = redis.call("RPOPLPUSH", KEYS[1], KEYS[2])
end

local m_key = "message:" .. m_id
redis.call("HMSET", m_key, "status", "received", "received_at", ARGV[1])
return redis.call("HGETALL", m_key)