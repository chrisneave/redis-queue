-- KEYS[1] = key containing incremental message ID
-- KEYS[2] = unique SET containing all received messages
-- KEYS[3] = unique key value for the message
-- KEYS[4] = key for the queue to push the message to
-- ARGV[1] = message body JSON
-- ARGV[2] = TIME value for requested_at

-- Check for existing messages
if redis.call("SISMEMBER", KEYS[2], KEYS[3]) == 1 then
  error("A message with the key %s already exists in set %s", KEYS[3], KEYS[2])
end

local m_id = redis.call("INCR", KEYS[1])
local m_key = "message:" .. m_id
redis.call("HSET", m_key, "status", "submitted")
redis.call("HSET", m_key, "body", ARGV[1])
redis.call("HSET", m_key, "requested_at", ARGV[2])
redis.call("LPUSH", KEYS[4], m_id)
return m_id