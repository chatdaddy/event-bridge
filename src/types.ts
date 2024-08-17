import { ChannelWrapper, Options } from 'amqp-connection-manager'
import type { PublishOptions } from 'amqp-connection-manager/dist/types/ChannelWrapper'
import type { Logger } from 'pino'

export type Serializer<Event> = {
    encode<T = any>(obj: T, event: Event): Buffer
    decode<T = any>(enc: Buffer, event: Event): T
}

export type EventData<M, E extends keyof M> = {
	event: E
	data: M[E][]
	ownerId: string
	/**
	 * Idempotency key for the message
	 */
	messageId?: string
}

type EventBatcherConfig = {
	/** regular flush interval */
	eventsPushIntervalMs?: number
	/** max events to take in before initiating a flush */
	maxEventsForFlush: number
}

export type EventBatcherOptions<M> = EventBatcherConfig & {
	/** actually flush the events */
	publish<E extends keyof M>(d: EventData<M, E>): Promise<void>
	logger: Logger
}

type DataWEvent<M> = {
	[key in keyof M]: {
		event: key
		data: M[key][]
	}
}

type SubscriptionData<M, T extends keyof M> = {
	ownerId?: string
	msgId: string
	logger: Logger
} & DataWEvent<M>[T]

export type EventSubscriptionListener<M, T extends keyof M> = (
	data: SubscriptionData<M, T>
) => Promise<void> | void

type AMQPBaseOptions<M> = {
	amqpUri: string
	/**
	 * Msg serializer
	 * @default V8Serializer
	 */
	serializer?: Serializer<keyof M>
	/**
	 * Add options to publish events
	 */
	publishOptions?: PublishOptions

	logger?: Logger

	batcherConfig?: EventBatcherConfig
}

export type AMQPSubscriberOptions<M> = AMQPBaseOptions<M> & {
	/**
	 * Queue name to process events. Will be automatically created
	 * if it doesn't exist w the provided options.
	 */
	queueName: string
	/**
	 * Events the worker shall listen for
	 */
	events: (keyof M)[]
	/**
	 * Event handler for the worker
	 */
	onEvent: EventSubscriptionListener<M, keyof M>
	/**
	 * Maximum number of messages this worker shall
	 * handle simultaneously
	 * @default 1
	 */
	maxMessagesPerWorker?: number
	/**
	 * Configuration for the queue. Changing any of these parameters
	 * after the queue has been created can possibly lead to the channel
	 * failing to establish a connection.
	 */
	queueConfig?: {
		/**
		 * Maximum number of consecutive retries for a message after its first
		 * delivery failure, before it's considered a failure &
		 * deleted from the queue.
		 *
		 * If delayedRetrySeconds is specified, the message will be
		 * retried again after the initial maxMessageRetries
		 * @default 2
		 */
		maxInitialRetries?: number
		/**
		 * Number of seconds to wait before retrying a failed message.
		 * Set to 0 to disable delayed retries.
		 * @default 1h
		 */
		delayedRetrySeconds?: number
		/**
		 * Maximum number of delayed retries for a message, after which
		 * the message is considered a failure & deleted from the queue.
		 * @default 3
		 */
		maxDelayedRetries?: number
		/**
		 * Queue options
		 */
		options?: Options.AssertQueue
	}
}

export type AMQPEventBridgeOptions<M> = AMQPSubscriberOptions<M>
	| AMQPBaseOptions<M>

export type AMQPEventBridge<M> = {
	/**
	 * Waits for the connection to be open
	 */
	waitForOpen(): Promise<void>
    close(): Promise<void>
    publish<E extends keyof M>(
		event: E,
		ownerId: string,
		data: M[E],
	): void
	/**
	 * Flushes all pending events
	 */
    flush(): Promise<void>

	__internal: {
		channel: ChannelWrapper
		/**
		 * Publishes the event immediately
		 */
		publishNow<E extends keyof M>(d: EventData<M, E>): Promise<void>
	}
}