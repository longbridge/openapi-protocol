# Longbridge Binary Protocol

Longbridge Binary Protocol is using for OpenAPI Socket Connection.

[More details](https://open.longbridge.com/docs/socket/protocol/overview)

For `Python` and `C++` user, we provide:

- [Python SDK](https://github.com/longbridge/openapi-sdk/tree/master/python)
- [C++ SDK](https://github.com/longbridge/openapi-sdk/tree/master/cpp)
- [Java SDK](https://github.com/longbridge/openapi-sdk/tree/master/java)
- [C SDK](https://github.com/longbridge/openapi-sdk/tree/master/c)
- [Rust SDK](https://github.com/longbridge/openapi-sdk/tree/master/rust)
- [NodeJS SDK](https://github.com/longbridge/openapi-sdk/tree/master/nodejs)

This repo want to show how to implement Longbridge Binary Protocol.

If you are `Gopher`, you can use the golang implementation to connect our socket gateway.

## Golang Implementation

Check code [here](https://github.com/longbridge/openapi-protocol/tree/main/go).

code structure:

- go - protocol definations
- go/client - client sample code
- go/v1 - protocol version 1 implement
- go/v2 - protocol version 2 implement

Example is [here](https://github.com/longbridge/openapi-protocol/tree/main/examples/go)
