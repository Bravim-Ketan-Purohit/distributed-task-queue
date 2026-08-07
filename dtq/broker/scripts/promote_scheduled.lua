-- promote_scheduled.lua
-- Move due entries from ZSET sched:<queue> into STREAM q:<queue>.
-- Atomic: the move cannot lose a task between read and write.
-- KEYS[1] = sched:<queue> (ZSET scored by wake time)
-- KEYS[2] = q:<queue> (STREAM)
-- ARGV[1] = current time (unix seconds, float as string)
-- ARGV[2] = max items to promote per tick
-- Returns: count of promoted tasks

local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local count = 0
for _, payload in ipairs(due) do
    redis.call('XADD', KEYS[2], '*', 'data', payload)
    redis.call('ZREM', KEYS[1], payload)
    count = count + 1
end
return count
