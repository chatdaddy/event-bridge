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

``` ts

type EventMap = {
	'resize-image-job': {
		imageUrl: string
	}
}

```

### Setup a Single Publisher

``` ts
import { makeAmqpEventBridge } from '@chatdaddy/event-bridge'

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
		timeout: 60_000
	},
})

// publish an event
bridge.publish(
	'resize-image-job',
	// owner of the event -- who owns the event & its data. Could be a user
	// ID, orginization ID, etc.
	// this is optional though -- you can leave it as a blank string too
	'user_1234',
	{ imageUrl: 'https://example.com/image.jpg' }
)

// flush events immediately
await bridge.flush()
```

The library batches events by the `owner` of the event, so if an owner produces multiple events in quick succession, they are batched together and published in a single publish operation.

If you'd really like to publish a single event in a single publish operation, you can do so by setting the `batcherConfig.maxEventsForFlush` to `1`.

### Setup with a Subscription to Process Events

``` ts
import { makeAmqpEventBridge } from '@chatdaddy/event-bridge'

const bridge = makeAmqpEventBridge<EventMap>({
	subscriptions: [
		{
			queueName: 'my-micro-service',
			// handle the event -- in case the async fn
			// rejects, it's counted as a failure
			async onEvent({ event, data, msgId }) {
				if(event === 'resize-image-job') {
					console.log(
						`Resising ${data.length} images`,
						'with message id', msgId
					)
				}
			},
			events: [
				'resize-image-job'
			],
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
				delayedRetrySeconds: 60 * 60
			}
		}
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
		timeout: 60_000
	},
})
```

### Publishing Directly to a Queue

Sometimes you might want to publish an event directly to a queue, without any batching or to another exchange. You can do so using the `sendDirect` method.

``` ts
// msg ID is returned for tracking the event -- if you want to
const { msgId } = await bridge.sendDirect({
	event: 'resize-image-job',
	data: [{ imageUrl: 'https://example.com/image.jpg' }],
	ownerId: 'some-owner-id',
	queueName: 'my-micro-service',
})
```

### Misc

``` ts
// gracefully shutdown the event bridge
// will process all currently processing events, and stop
// accepting new events
await bridge.close()
```

## How Delayed Retry Works

We utilise a dead-letter exchange to handle delayed retries.
1. Let's suppose our queue is named `fun`, which has a `x-delivery-limit` of `N` -- which means the message will be retried at most `N` times before being discarded.
	- A `x-dead-letter-exchange` argument is also provided to the queue -- this is the exchange where the message will be sent in case `N` retries are exhausted. 
	- This exchange is named `fun_dlx`, it's exclusive to the `fun` queue.
2. Now, assume some message in `fun` fails to be processed, it is pushed to the `fun_dlx` exchange.
3. We'll also have another queue `fun_dlx_queue` which is exclusively bound to the `fun_dlx` exchange.
	- This queue will have a `x-message-ttl` of `T` milliseconds -- which means any message in the queue will last for `T` milliseconds before being discarded or pushed to another exchange, if a `x-dead-letter-exchange` argument is provided.
	- We'll provide a `x-dead-letter-exchange` argument to the `fun_dlx_queue` -- this is the exchange where the message will be sent in case `T` milliseconds have passed. This exchange is named `fun_dlx_back_to_queue`.
	- `fun_dlx_back_to_queue` is bound to the `fun` queue.
4. Thus, if a message in `fun` fails to be processed, it is pushed to the `fun_dlx` exchange, which leads to the `fun_dlx_queue` where it waits for `T` milliseconds before being pushed back to the `fun_dlx_back_to_queue` exchange, and then finally back to the `fun` queue.
5. If the message fails after the delayed retry, it is requeued `M` times, where `M` is the `maxDelayedRetries` provided in the queue configuration.