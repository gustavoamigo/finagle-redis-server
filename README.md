# finagle-redis-server

A simple Redis Clone using Finagle Architecture

Commands coded so far:

   * GET
   * SET (no TTL yet)
   * MGET
   * APPEND
   * INCR
   * PING
    

To run locally, right now it is binded to port 6380 
    
    run com.twitter.finagle.redis.server.RedisServer
     
If you have redis installed, you can benchmark it using the following commands
   
    redis-benchmark -t ping,set,get,incr -q -p 6380
   
