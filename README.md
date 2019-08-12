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