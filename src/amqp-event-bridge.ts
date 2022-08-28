import AMQP from 'amqp-connection-manager'
import type { ConfirmChannel, ConsumeMessage } from 'amqplib'
import { randomBytes } from 'crypto'
import P, { Logger } from 'pino'
import makeEventDebouncer, { EventDebouncerOptions } from './make-event-debouncer'
import { Serializer, V8Serializer } from './serializer'

type SubscriptionListener<M> = (data: M[], ownerId: string, msgId: string) => Promise<void> | void

type Subscription = {
	queueName: string
	consumerTag?: string
	listeners: { [exchange: string]: SubscriptionListener<any> }
}

type SubscriptionOptions<M, E extends keyof M> = {
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
	maxMessagesPerWorker?: number
	logger?: Logger
	serializer?: Serializer<Event>
} & Pick<EventDebouncerOptions<any>, 'eventsPushIntervalMs' | 'maxEventsForFlush'>

export type AMQPEventBridge<M> = ReturnType<typeof makeAmqpEventBridge<M>>

export function makeAmqpEventBridge<M>(
	{
		amqpUri,
		maxMessagesPerWorker,
		logger: _logger,
		maxEventsForFlush,
		eventsPushIntervalMs,
		serializer
	}: AMQPEventBridgeOptions<keyof M>
) {
	type E = keyof M

	const { encode, decode } = serializer || V8Serializer

	const logger = (_logger || P({ level: 'trace' }))
	const subscriptions: { [exchange: string]: Promise<Subscription> | undefined } = {}
	const exchangesAsserted = new Set<string>()

	const eventDebouncer = makeEventDebouncer<M>({
		publish,
		logger,
		maxEventsForFlush,
		eventsPushIntervalMs
	})

	const conn = AMQP.connect([amqpUri], { })
	const channel = conn.createChannel({ setup: setupMain })

	let opened = false

	channel.on('error', error => {
		logger.error(`error in channel: ${error}`)
	})
	conn.on('disconnect', (arg) => {
		logger.error(`error in connection: ${arg.err}`)
		opened = false
	})

	async function waitForOpen() {
		if(!opened) {
			await channel.waitForConnect()
		}
	}

	async function setupMain(channel: ConfirmChannel) {
		if(maxMessagesPerWorker) {
			await channel.prefetch(maxMessagesPerWorker)
		}

		logger.info('opened channel')
		opened = true
	}

	async function consume(sub: Subscription) {
		const { queueName } = sub
		// assert queue exists
		await channel.assertQueue(queueName, { autoDelete: true })
		logger.debug({ queueName }, 'asserted queue')
		// start the subscription
		const { consumerTag } = await channel.consume(
			queueName,
			msg => consumerHandler(sub, msg),
			{ noAck: false }
		)
		sub.consumerTag = consumerTag
		logger.debug({ queueName, consumerTag }, 'consuming events')
	}

	async function consumerHandler(sub: Subscription, msg: ConsumeMessage) {
		const exchange = msg.fields.exchange
		const msgId = msg.properties.correlationId
		// owner ID is in the routing key
		const ownerId = msg.fields.routingKey

		try {
			const data = decode(msg.content, exchange as E)
			const listener = sub?.listeners[exchange]
			if(listener) {
				await listener?.(data, ownerId, msgId)

				logger.trace(
					{ exchange, id: msgId, ownerId },
					'handled msg'
				)
			} else {
				logger.warn({ fields: msg.fields }, 'recv msg with no handler')
			}

			channel.ack(msg)
		} catch(error) {
			logger.error(
				{ id: msgId, trace: error.stack },
				'error in handling msg'
			)
			channel.nack(msg, undefined, true)
		}
	}

	const createSubscription = async(queueName: string) => {
		const binding: Subscription = {
			queueName,
			listeners: { },
		}
		await consume(binding)
		return binding
	}

	const addListenerToSubscription = async(
		sub: Subscription,
		exchangeName: string,
		routingKey: string,
		listener: SubscriptionListener<any>
	) => {
		sub.listeners[exchangeName] = listener
		await assertExchangeIfRequired(exchangeName, channel)
		await channel.bindQueue(sub.queueName, exchangeName, routingKey)

		logger.trace({ exchangeName, queueName: sub.queueName, routingKey }, 'bound to exchange')
	}

	async function removeSubscriptionListener(
		sub: Subscription,
		exchangeName: string,
		pattern: string,
		unbind: boolean
	) {
		if(unbind) {
			await channel.unbindExchange(sub.queueName, exchangeName, pattern)
			logger.trace({ exchangeName, queue: sub.queueName }, 'unbound exchange')
		}

		delete sub.listeners[exchangeName]
	}

	async function clearSubscription(sub: Subscription) {
		const { consumerTag, queueName } = sub
		if(consumerTag) {
			await channel.cancel(consumerTag)
			sub.consumerTag = undefined
			logger.trace({ consumerTag, queueName }, 'removed subscription')
		}
	}

	async function assertExchangeIfRequired(exchangeName: string, channel) {
		if(!exchangesAsserted.has(exchangeName)) {
			// topic exchanges so we can match on routing key
			await channel.assertExchange(exchangeName, 'topic')
			exchangesAsserted.add(exchangeName)
		}
	}

	async function publish<Event extends E>(event: Event, data: M[Event][], ownerId: string) {
		await waitForOpen()
		const exchange = event.toString()
		await assertExchangeIfRequired(exchange, channel)

		const messageId = randomBytes(8).toString('hex')

		await channel.publish(
			exchange,
			ownerId,
			encode(data, event),
			{
				correlationId: messageId,
				contentType: 'application/octet-stream'
			}
		)

		logger.trace({ exchange, items: data.length, ownerId }, 'published')
	}

	return {
		...eventDebouncer,
		waitForOpen,
		async close() {
			eventDebouncer.close()
			await Promise.all(
				Object.keys(subscriptions).map(async k => {
					const v = subscriptions[k]
					await clearSubscription(await v!)
				})
			)
			try {
				await channel.close()
				await conn.close()
			} catch(error) {

			}

			logger.info('closed')
		},
		/**
		 * Subscribe to an event
		 * @param event the event to listen for
		 * @param ownerId optionally, only listen for events of
		 * this event owner (user, team, workspace -- however you want to term it)
		 * @param listener event handler
		 * @returns
		 */
		async subscribe<Event extends E>({
			ownerId,
			subscriptionId,
			event,
			listener
		}: SubscriptionOptions<M, Event>) {
			await waitForOpen()
			const key = subscriptionId || makeRandomQueueName()
			// match specific owner if specified
			// or wildcard match
			const matchPattern = ownerId || '*'

			if(!subscriptions[key]) {
				logger.debug({ queueName: key }, 'creating subscription')
				subscriptions[key] = createSubscription(key)
					.catch(err => {
						delete subscriptions[key]
						throw err
					})
			}

			const subPromise = subscriptions[key]!
			const newListenerPromise = subPromise
				.then(
					async sub => {
						await addListenerToSubscription(sub, event.toString(), matchPattern, listener)
						return sub
					}
				)
			subscriptions[key] = newListenerPromise
				.catch(() => {
					// if the thing errored out
					// revert to existing sub
					subscriptions[key] = subPromise
					return subPromise
				})

			await newListenerPromise

			return async(unbind = false) => {
				const existingSub = subscriptions[key]
				const removePromise = existingSub?.then(
					async sub => {
						await removeSubscriptionListener(sub, event.toString(), matchPattern, unbind)

						if(!Object.keys(sub.listeners).length) {
							await clearSubscription(sub)
							delete subscriptions[key]
							logger.trace({ queue: sub.queueName }, 'cleared sub')
						}

						return sub
					}
				)
				// queue the remove promise
				// revert to existing sub if it errors out
				subscriptions[key] = removePromise!.catch(() => existingSub!)

				await removePromise
			}
		},
		subscriptions: () => subscriptions,
	}
}

function makeRandomQueueName() {
	return `rand-${randomBytes(4).toString('hex')}`
}