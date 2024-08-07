import AMQP from 'amqp-connection-manager'
import { PublishOptions } from 'amqp-connection-manager/dist/types/ChannelWrapper'
import type { ConfirmChannel, ConsumeMessage } from 'amqplib'
import { randomBytes } from 'crypto'
import P from 'pino'
import makeEventDebouncer from './make-event-debouncer'
import { V8Serializer } from './serializer'
import { AMQPEventBridge, AMQPEventBridgeOptions, EventData, Subscription, SubscriptionListener, SubscriptionOptions } from './types'
import { makeRandomMsgId } from './utils'

const DEFAULT_PUBLISH_OPTIONS: PublishOptions = {
	contentType: 'application/octet-stream',
	persistent: true,
}

export function makeAmqpEventBridge<M>(
	{
		amqpUri,
		maxMessagesPerWorker,
		logger: _logger,
		maxEventsForFlush,
		eventsPushIntervalMs,
		serializer,
		defaultPublishOptions,
	}: AMQPEventBridgeOptions<keyof M>
): AMQPEventBridge<M> {
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

	channel.on('error', err => {
		logger.error({ err }, 'error in channel')
	})
	conn.on('disconnect', (arg) => {
		logger.error({ err: arg.err }, 'error in connection')
		opened = false
	})

	return {
		__internal: {
			channel,
		},
		...eventDebouncer,
		waitForOpen,
		async close() {
			// flush any pending events
			await eventDebouncer.flush()
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
						await removeSubscriptionListener(
							sub,
							event.toString(),
							matchPattern,
							unbind
						)

						if(!Object.keys(sub.listeners).length) {
							await clearSubscription(sub)
							delete subscriptions[key]
							logger.trace(
								{ queue: sub.queueName },
								'cleared sub'
							)
						}

						return sub
					}
				)
				// queue the remove promise
				// revert to existing sub if it errors out
				subscriptions[key] = removePromise!
					.catch(() => existingSub!)

				await removePromise
			}
		},
		subscriptions: () => subscriptions,
	}


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
				logger.warn(
					{ fields: msg.fields },
					'recv msg with no handler'
				)
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

	async function createSubscription(queueName: string) {
		const binding: Subscription = {
			queueName,
			listeners: { },
		}
		await consume(binding)
		return binding
	}

	async function addListenerToSubscription(
		sub: Subscription,
		exchangeName: string,
		routingKey: string,
		listener: SubscriptionListener<any>
	) {
		sub.listeners[exchangeName] = listener
		await assertExchangeIfRequired(exchangeName, channel)
		await channel.bindQueue(sub.queueName, exchangeName, routingKey)

		logger.trace(
			{ exchangeName, queueName: sub.queueName, routingKey },
			'bound to exchange'
		)
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

	async function publish<Event extends E>({
		event,
		data,
		ownerId,
		messageId = makeRandomMsgId()
	}: EventData<M, Event>) {
		await waitForOpen()
		const exchange = event.toString()
		await assertExchangeIfRequired(exchange, channel)

		await channel.publish(
			exchange,
			ownerId,
			encode(data, event),
			{
				correlationId: messageId,
				...DEFAULT_PUBLISH_OPTIONS,
				...defaultPublishOptions,
			}
		)

		logger.trace(
			{ exchange, items: data.length, ownerId, messageId },
			'published'
		)
	}
}

function makeRandomQueueName() {
	return `rand-${randomBytes(4).toString('hex')}`
}