-- release_lease.lua
-- Compare-and-delete: only release if we still own the lease.
-- A worker can never delete a lease it no longer owns.
-- KEYS[1] = lease:<task_id>
-- ARGV[1] = expected value (worker_id:fence)
-- Returns: 1 if deleted, 0 if not owned

local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
else
    return 0
end
