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

export type EventDebouncerOptions<M> = {
	/** actually flush the events */
	publish<E extends keyof M>(d: EventData<M, E>): Promise<void>
	logger: Logger
	/** regular flush interval */
	eventsPushIntervalMs?: number
	/** max events to take in before initiating a flush */
	maxEventsForFlush: number
}

export type SubscriptionListener<M> = (
	data: M[],
	ownerId: string,
	msgId: string
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

export type AMQPEventBridgeOptions<Event> = {
	amqpUri: string
	/**
	 * Maximum number of messages this worker shall
	 * handle simultaneously
	 */
	maxMessagesPerWorker?: number
	logger?: Logger
	serializer?: Serializer<Event>
	defaultPublishOptions?: PublishOptions
} & Pick<EventDebouncerOptions<any>, 'eventsPushIntervalMs' | 'maxEventsForFlush'>

export type AMQPEventBridge<M> = {
	/**
	 * Waits for the connection to be open
	 */
	waitForOpen(): Promise<void>
    close(): Promise<void>
	/**
	 * Subscribe to an event
	 * @param event the event to listen for
	 * @param ownerId optionally, only listen for events of
	 * this event owner (user, team, workspace -- however you want to term it)
	 * @param listener event handler
	 * @returns fn to cancel the subscription, optional parameter
	 *  to unbind the queue ( @default false )
	 */
    subscribe<Event extends keyof M>(
		opts: SubscriptionOptions<M, Event>
	): Promise<(unbind?: boolean) => Promise<void>>
    subscriptions(): { [id: string]: Promise<Subscription> | undefined }
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