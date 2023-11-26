

--参数:1.用户id 2.优惠券id 3.订单id
local userId = ARGV[1]
local voucherId = ARGV[2]
local orderId = ARGV[3]

--key:库存 订单
local stockKey = "seckill:stock:" .. voucherId
local orderKey = "seckill:order:" .. voucherId

--判断库存是否充足
if tonumber(redis.call("get",stockKey)) <= 0 then
    return 1
end

--判断用户是否已经下过订单
if redis.call("sismember",orderKey,userId) == 1 then
    return 2
end


--扣库存,保存用户订单到set集合
redis.call("incrby",stockKey,-1)
redis.call("sadd",orderKey,userId)

--发送消息到消息队列 xadd stream.orders * k1 v1 k2 v2...
redis.call("xadd","stream.orders","*","userId",userId,"voucherId",voucherId,"id",orderId)

return 0