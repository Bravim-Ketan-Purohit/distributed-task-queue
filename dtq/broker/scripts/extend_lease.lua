-- extend_lease.lua
-- Heartbeat: extend lease TTL only if we still own it.
-- KEYS[1] = lease:<task_id>
-- ARGV[1] = expected value (worker_id:fence)
-- ARGV[2] = new TTL in ms
-- Returns: 1 if extended, 0 if lost

local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
else
    return 0
end
