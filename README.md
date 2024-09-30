# Event Bridge

AMQP based reliable event handling in a services based architecture. The library aims to:

- Provide a simple, opinionated, type-safe mechanism for event handling
- Ensure reliable event delivery, automatically retrying failed events
- Graceful handling of server shutdowns -- ensuring events being processed are waited for completion
- Upon an initial failure of event consumption, the event is retried with an constant backoff strategy
- Batch publish events to improve processing throughput + keep retrying failed publishes

## Use Cases

1. **Job Queues**: You can use this library to create a job queue, where you publish jobs to a queue, and have multiple services consume the jobs with limited concurrency.
   - Eg. You can use this to create a system where you publish jobs like `resize-image-job`, `send-email-job`, `generate-pdf-job`, etc. and have multiple services consume these jobs to perform the actual work.
2. **Event Sourcing**: You can use this library to create an event sourcing system, where you publish events to a queue, and have multiple services consume the events.
   - Eg. You can use this to create a system where you publish events like `user-created`, `user-updated`, `user-deleted`, etc. and have multiple services consume these events to update their own databases, or perform some other action.

## Installation

```bash
npm install git+https://github.com/chatdaddy/event-bridge.git
```

## Testing

1. Clone the repository
2. Start the RabbitMQ server. We've a docker-compose file for that. Run `docker-compose up -d`
3. Run the tests using `npm run test`

## Usage

First, you'd want to define the types of events you're going to handle. This is done using a type alias, where the key is the event name and the value is the type of data associated with the event.

```ts
type EventMap = {
  "resize-image-job": {
    imageUrl: string;
  };
};
```

### Setup a Single Publisher

```ts
import { makeAmqpEventBridge } from "@chatdaddy/event-bridge";

const bridge = makeAmqpEventBridge<EventMap>({
  batcherConfig: {
    // automatically push events every 350ms
    // automatic publish after an interval is disabled
    // by default, or by setting to 0
    eventsPushIntervalMs: 350,
    // auto publish when the number of events reaches 500
    maxEventsForFlush: 500,
    // max retries for a failed publish
    maxRetries: 3,
  },
  publishOptions: {
    // timeout for the publish operation
    timeout: 60_000,
  },
});

// publish an event
bridge.publish(
  "resize-image-job",
  // owner of the event -- who owns the event & its data. Could be a user
  // ID, orginization ID, etc.
  // this is optional though -- you can leave it as a blank string too
  "user_1234",
  { imageUrl: "https://example.com/image.jpg" }
);

// flush events immediately
await bridge.flush();
```

The library batches events by the `owner` of the event, so if an owner produces multiple events in quick succession, they are batched together and published in a single publish operation.

If you'd really like to publish a single event in a single publish operation, you can do so by setting the `batcherConfig.maxEventsForFlush` to `1`.

### Setup with a Subscription to Process Events

```ts
import { makeAmqpEventBridge } from "@chatdaddy/event-bridge";

const bridge = makeAmqpEventBridge<EventMap>({
  subscriptions: [
    {
      queueName: "my-micro-service",
      // handle the event -- in case the async fn
      // rejects, it's counted as a failure
      async onEvent({ event, data, msgId }) {
        if (event === "resize-image-job") {
          console.log(
            `Resising ${data.length} images`,
            "with message id",
            msgId
          );
        }
      },
      events: ["resize-image-job"],
      // number of messages to process concurrently
      // on a single worker
      maxMessagesPerWorker: 10,
      queueConfig: {
        // will retry the event at most 2 times
        // immediately after the initial failure
        maxInitialRetries: 2,
        // max number of times the event will be retried
        // with the specified delay. After this, the event
        // will be discarded. Set to 0 to disable delayed retries.
        maxDelayedRetries: 3,
        // retry after 1hr
        delayedRetrySeconds: 60 * 60,
      },
    },
  ],
  batcherConfig: {
    // automatically push events every 350ms
    // or when the number of events reaches 500
    eventsPushIntervalMs: 350,
    maxEventsForFlush: 500,
    // max retries for a failed publish
    maxRetries: 3,
  },
  publishOptions: {
    timeout: 60_000,
  },
});
```

### Publishing Directly to a Queue

Sometimes you might want to publish an event directly to a queue, without any batching or to another exchange. You can do so using the `sendDirect` method.

```ts
// msg ID is returned for tracking the event -- if you want to
const { msgId } = await bridge.sendDirect({
  event: "resize-image-job",
  data: [{ imageUrl: "https://example.com/image.jpg" }],
  ownerId: "some-owner-id",
  queueName: "my-micro-service",
});
```

### Misc

```ts
// gracefully shutdown the event bridge
// will process all currently processing events, and stop
// accepting new events
await bridge.close();
```

## Delayed Retry

We utilise a dead-letter exchange to handle delayed retries.

---

#### Queue and Exchange Glossary

Supposing a queue `fun` with a `x-delivery-limit` of `N` (which means the message will be retried at most `N` times before being discarded). It has an argument `x-dead-letter-exchange` argument that points to the exchange where failed messages are routed after `N` retries

- `fun_dlx`: Dead-letter exchange for the `fun` queue, where failed messages are routed after `N` retries. It's exclusive to the `fun` queue
- `fun_dlx_queue`: Queue bound to `fun_dlx` with a `x-message-ttl` of `T` milliseconds. Has an argument `x-dead-letter-exchange` points to exchange where failed messages are sent
- `x-message-ttl`: Time-to-live (`T` milliseconds) for messages in `fun_dlx_queue`
- `x-dead-letter-exchange`: Argument that points to exchange where failed messages are sent
- `fun_dlx_back_to_queue`: Exchange where the message will be sent from `fun_dlx_queue` in case `T` milliseconds have passed
- `M`: Maximum number of delayed retries for a message (maxDelayedRetries)

---

#### How it works

1. The `fun` queue has a retry limit (`x-delivery-limit`) of `N` and is configured with a dead-letter exchange (`fun_dlx`)
2. When a message in `fun` fails to process `N` times, it is routed to the `fun_dlx` exchange
3. The `fun_dlx_queue` is bound to `fun_dlx` and holds failed messages for `T` milliseconds (`x-message-ttl`) before being discarded or pushed to another exchange if a `x-dead-letter-exchange` argument is provided
4. In case `T` milliseconds have passed, messages are pushed from `fun_dlx_queue` to the `fun_dlx_back_to_queue` exchange (this is specified by `x-dead-letter-exchange` argument of `fun_dlx_queue`), which in turn requeues it to the `fun` queue
6. If a message fails again after being requeued, it can be requeued up to M times (`maxDelayedRetries`)
