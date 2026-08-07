-- acquire_lease.lua
-- Atomically: INCR fence:seq, SET lease:<task_id> only if not already held.
-- KEYS[1] = fence:seq
-- KEYS[2] = lease:<task_id>
-- ARGV[1] = worker_id
-- ARGV[2] = lease_ms
-- Returns: fence token (int) on success, 0 if already leased

local fence = redis.call('INCR', KEYS[1])
local acquired = redis.call('SET', KEYS[2], ARGV[1] .. ':' .. tostring(fence), 'NX', 'PX', ARGV[2])
if acquired then
    return fence
else
    -- Undo the fence increment since we didn't use it
    redis.call('DECR', KEYS[1])
    return 0
end
