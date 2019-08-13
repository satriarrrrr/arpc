# ARPC

Implementation RPC over AMQP using Go and RabbitMQ.

## Feature
- Client
  - [x] Send request to a spesific Queue
  - [x] Pairing response with its request
  - [ ] Handle timeout per request
  - [x] Gracefully close the client
    - [x] Wait all request got response
    - [x] Force to close channel after waiting in spesific time
- Server
  - [ ] Receive request
    - [ ] Handle different type of request (like http router)
  - [ ] Return response

AMQP                    vs  HTTP
wire-level protocol         general purpose protocol 
asynchronous                synchronous
can guarantee the           drop the message when server is down
message is delivered

Things to Think!
1. On the client we should handle duplicate response
2. On the server we should make sure idempotency
3. Timeout for each request
4. Gracefully shutdown the client
5. Documentation of the method (address, input, and output)