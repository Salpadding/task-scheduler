local main = function()
    local running_set = KEYS[1]
    local waiting_set = KEYS[2]
    local waiting_list = KEYS[3]

    local concurrent = tonumber(ARGV[1])
    local task_set_key = ARGV[2]

    if type(task_set_key) == 'string' and #task_set_key > 0 then
        redis.call('SADD', waiting_set, task_set_key)
        redis.call('RPUSH', waiting_list, task_set_key)
    end

    local ret = {}

    while redis.call('SCARD', running_set) < concurrent do
        local poped = redis.call('LPOP', waiting_list)

        if type(poped) ~= 'string' then return ret end

        redis.call('SREM', waiting_set, poped)
        redis.call('SADD', running_set, poped)
        ret[#ret + 1] = poped
    end
    return ret
end

return main()
