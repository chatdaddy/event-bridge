# Event Bridge

AMQP based pub/sub event manager for reliable event handling. The library aims to:
- Provide a simple, opinionated, type-safe mechanism for event handling
- Ensure reliable event delivery, automatically retrying failed events
- Upon an initial failure of event consumption, the event is retried with an constant backoff strategy
- Batch publish events to improve processing throughput + keep retrying failed publishes

## Installation

```bash
npm install git+https://github.com/chatdaddy/event-bridge.git
```

## Testing

1. Clone the repository
2. Start the RabbitMQ server. We've a docker-compose file for that. Run `docker-compose up -d`
3. Run the tests using `npm run test`