import { ChannelWrapper } from 'amqp-connection-manager'
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

type EventDebouncerConfig = {
	/** regular flush interval */
	eventsPushIntervalMs?: number
	/** max events to take in before initiating a flush */
	maxEventsForFlush: number
}

export type EventDebouncerOptions<M> = EventDebouncerConfig & {
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

export type SubscriptionListener<M, T extends keyof M> = (
	data: SubscriptionData<M, T>
) => Promise<void> | void

export type AMQPEventBridgeOptions<M> = {
	amqpUri: string
	/**
	 * Worker group ID -- all workers with the same
	 * workerId will share the same queue.
	 * Queue will only be setup if onEvent is provided
	 */
	workerId: string
	/**
	 * Events the worker shall listen for
	 */
	events: (keyof M)[]

	onEvent?: SubscriptionListener<M, keyof M>
	/**
	 * Maximum number of messages this worker shall
	 * handle simultaneously
	 * @default 1
	 */
	maxMessagesPerWorker?: number
	/**
	 * Maximum number of retries for a message after its first
	 * delivery failure, before it's considered a failure &
	 * deleted from the queue
	 * @default 3
	 */
	maxMessageRetries?: number
	logger?: Logger
	/**
	 * Msg serializer
	 * @default V8Serializer
	 */
	serializer?: Serializer<keyof M>
	/**
	 * Add options to publish events
	 */
	publishOptions?: PublishOptions
	debouncerConfig?: EventDebouncerConfig
}

export type AMQPEventBridge<M> = {
	/**
	 * Waits for the connection to be open
	 */
	waitForOpen(): Promise<void>
    close(): Promise<void>
    publish<E extends keyof M>(
		event: E,
		data: M[E],
		ownerId: string
	): void
	/**
	 * Flushes all pending events
	 */
    flush(): Promise<void>

	__internal: {
		channel: ChannelWrapper
	}
}