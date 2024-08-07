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

type SubscriptionData<M, T extends keyof M> = {
	event: T
	data: M[T][]
	ownerId?: string
	msgId: string
	logger: Logger
}

export type SubscriptionListener<M> = (
	data: SubscriptionData<M, keyof M>
) => Promise<void> | void

export type Subscription = {
	queueName: string
	consumerTag?: string
	listeners: { [exchange: string]: SubscriptionListener<any> }
}

export type SubscriptionOptions<M, E extends keyof M> = {
	/**
	 * only listen for events for this particular owner;
	 * if not provided, listen for events for all owners
	 * */
	ownerId?: string
	/**
	 * specify the queue to use;
	 * only one of all workers subscribing
	 * with the same subscription ID
	 * will receive the event
	 */
	subscriptionId?: string
	event: E
	listener: SubscriptionListener<M[E]>
}

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

	onEvent?: SubscriptionListener<M>
	/**
	 * Maximum number of messages this worker shall
	 * handle simultaneously
	 * @default 1
	 */
	maxMessagesPerWorker?: number
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