-- KEYS[1] = KEY of receive queue
-- KEYS[2] = KEY of finish queue
-- KEYS[3] = KEY of message
-- KEYS[4] = KEY of concurrent message set
-- ARGV[1] = Value of the status field
-- ARGV[2] = Value of the finished_at field

local m_id = redis.call("LREM", KEYS[1], 1, KEYS[3])
if m_id > 0 then
  redis.call("LPUSH", KEYS[2], KEYS[3])
  local m_key = "message:" .. m_id
  local concurrent_id = redis.call("HGET", m_key, "concurrent_id")
  redis.call("HMSET", m_key, "status", ARGV[1], "finished_at", ARGV[2])
  redis.call("SREM", KEYS[4], concurrent_id)
  return m_id
end