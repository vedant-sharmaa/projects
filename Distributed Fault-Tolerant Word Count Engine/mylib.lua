#!lua name=mylib
redis.register_function('atomic_increment_and_acknowledge', function(keys, args)
    local count_key = keys[1]
    local stream_key = keys[2]
    local group_name = keys[3]
    local message_id = keys[4]
    local word_count = cjson.decode(args[1])

    -- Acknowledge the message
    local ack_result = redis.call('XACK', stream_key, group_name, message_id)

    if ack_result == 1 then
        -- Increment word counts only if the message was successfully acknowledged
        for word, count in pairs(word_count) do
            redis.call('ZINCRBY', count_key, count, word)
        end
        return 'OK'
    else
        -- If acknowledgment fails, do not update word counts
        return 'FAIL'
    end
end)