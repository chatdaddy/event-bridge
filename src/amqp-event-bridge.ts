import AMQP, { ChannelWrapper, Options } from 'amqp-connection-manager'
import { PublishOptions } from 'amqp-connection-manager/dist/types/ChannelWrapper'
import type { ConfirmChannel, ConsumeMessage } from 'amqplib'
import P from 'pino'
import makeEventBatcher from './make-event-batcher'
import { V8Serializer } from './serializer'
import { AMQPEventBridge, AMQPEventBridgeOptions, EventData } from './types'
import { makeUqMessageId, parseMessageId } from './utils'

const DEFAULT_PUBLISH_OPTIONS: PublishOptions = {
	contentType: 'application/octet-stream',
	persistent: true,
	timeout: 5_000,
}

// six hours
const MSG_TIMEOUT_S = 6 * 60 * 60

const DEFAULT_QUEUE_OPTIONS: Options.AssertQueue = {
	autoDelete: false,
	durable: true,
	exclusive: false,
	messageTtl: MSG_TIMEOUT_S,
	arguments: {
		// quorum queues allow for delivery limits
		// and retry count tracking
		'x-queue-type': 'quorum'
	}
}

const DEFAULT_MSGS_TO_FLUSH = 250

export function makeAmqpEventBridge<M>(
	{
		amqpUri,
		workerId,
		events,
		onEvent,
		maxMessagesPerWorker,
		logger = P(),
		serializer,
		publishOptions,
		queueOptions,
		batcherConfig,
		maxMessageRetries = 3
	}: AMQPEventBridgeOptions<M>
): AMQPEventBridge<M> {
	type E = keyof M

	const { encode, decode } = serializer || V8Serializer
	const exchangesAsserted = new Set<string>()

	const batcher = makeEventBatcher<M>({
		publish,
		logger,
		maxEventsForFlush: DEFAULT_MSGS_TO_FLUSH,
		...batcherConfig,
	})

	const conn = AMQP.connect([amqpUri], { })
	const channel = conn.createChannel({ setup: setupMain })

	let opened = false
	let listenerTag: string | undefined

	channel.on('error', err => {
		logger.error({ err }, 'error in channel')
	})
	conn.on('disconnect', (arg) => {
		logger.error({ err: arg.err }, 'error in connection')
		opened = false
	})
	conn.on('connectFailed', (err) => {
		logger.error({ err }, 'connect failed')
	})

	return {
		__internal: { channel },
		...batcher,
		waitForOpen,
		async close() {
			// flush any pending events
			await batcher.flush()

			try {
				if(listenerTag) {
					await channel.cancel(listenerTag)
					listenerTag = undefined
				}

				await channel.close()
				await conn.close()
			} catch(error) {

			}

			logger.info('closed')
		},
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

		if(onEvent) {
			await startListening(channel)
		}
	}

	async function startListening(channel: ConfirmChannel) {
		await channel.assertQueue(
			workerId,
			{
				...DEFAULT_QUEUE_OPTIONS,
				...queueOptions,
				arguments: {
					...DEFAULT_QUEUE_OPTIONS.arguments,
					...queueOptions?.arguments,
					'x-delivery-limit': maxMessageRetries
				}
			}
		)

		logger.debug({ workerId }, 'asserted queue')

		for(const event of events) {
			const exchange = String(event)
			await assertExchangeIfRequired(exchange, channel)
			await channel.bindQueue(workerId, exchange, '*')
		}

		logger.debug({ workerId, events }, 'bound queue to exchanges')
		// start the subscription
		const { consumerTag } = await channel.consume(
			workerId,
			consumerHandler,
			{ noAck: false }
		)

		listenerTag = consumerTag
		logger.debug({ consumerTag }, 'consuming events')
	}

	async function consumerHandler(msg: ConsumeMessage) {
		const exchange = msg.fields.exchange as E
		const msgId = msg.properties.messageId
		// owner ID is in the routing key
		const ownerId = msg.fields.routingKey
		const retryCount = +(
			msg.properties.headers?.['x-delivery-count'] || 0
		)

		const _logger = logger.child({
			exchange,
			ownerId,
			msgId,
			retryCount: retryCount || undefined,
		})

		// if the delivery tag > 1 && not redelivered,
		// then it's a duplicate message
		// if(retryCount && !msg.fields.redelivered) {
		// 	_logger.info('ignored duplicate msg')
		// 	channel.ack(msg)
		// 	return
		// }

		let data: any
		try {
			data = decode(msg.content, exchange)

			const parsed = parseMessageId(msgId)

			_logger.info({ eventTs: parsed?.dt, data }, 'handling msg')

			await onEvent!({
				ownerId,
				msgId,
				logger: _logger,
				data,
				event: exchange,
			})

			_logger.trace(
				{ exchange, id: msgId, ownerId },
				'handled msg'
			)

			channel.ack(msg)
		} catch(err) {
			_logger.error({ err }, 'error in handling msg')
			channel.nack(msg, undefined, true)
		}
	}

	async function assertExchangeIfRequired(
		exchangeName: string,
		channel: ChannelWrapper | ConfirmChannel
	) {
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
		messageId = makeUqMessageId()
	}: EventData<M, Event>) {
		await waitForOpen()
		const exchange = event.toString()
		await assertExchangeIfRequired(exchange, channel)

		await channel.publish(
			exchange,
			ownerId,
			encode(data, event),
			{
				messageId: messageId,
				...DEFAULT_PUBLISH_OPTIONS,
				...publishOptions,
			}
		)

		logger.trace(
			{ exchange, items: data.length, ownerId, messageId },
			'published'
		)
	}
}