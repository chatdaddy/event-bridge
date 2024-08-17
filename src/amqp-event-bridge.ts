import AMQP, { ChannelWrapper, Options } from 'amqp-connection-manager'
import { PublishOptions } from 'amqp-connection-manager/dist/types/ChannelWrapper'
import type { ConfirmChannel, ConsumeMessage } from 'amqplib'
import P from 'pino'
import { makeEventBatcher } from './make-event-batcher'
import { V8Serializer } from './serializer'
import { AMQPEventBridge, AMQPEventBridgeOptions, AMQPSubscriberOptions, EventData } from './types'
import { makeUqMessageId, parseMessageId } from './utils'

const DEFAULT_PUBLISH_OPTIONS: PublishOptions = {
	contentType: 'application/octet-stream',
	persistent: true,
	// 3s timeout
	timeout: 3_000,
}

// six hours
const MSG_TIMEOUT_MS = 6 * 60 * 60 * 1000

const DEFAULT_QUEUE_OPTIONS: Options.AssertQueue = {
	autoDelete: false,
	durable: true,
	exclusive: false,
	messageTtl: MSG_TIMEOUT_MS,
	arguments: {
		// quorum queues allow for delivery limits
		// and retry count tracking
		'x-queue-type': 'quorum',
	}
}

const DEFAULT_MSGS_TO_FLUSH = 250

const DEFAULT_RETRY_DELAY_S = 60 * 60 // 1h

export function makeAmqpEventBridge<M>(
	{
		amqpUri,
		logger = P(),
		serializer,
		publishOptions,
		batcherConfig,
		...rest
	}: AMQPEventBridgeOptions<M>
): AMQPEventBridge<M> {
	type E = keyof M

	const {
		queueName = '',
		events = [],
		onEvent,
		maxMessagesPerWorker,
		queueConfig: {
			maxInitialRetries = 2,
			delayedRetrySeconds = DEFAULT_RETRY_DELAY_S,
			maxDelayedRetries = 3,
			options: queueOptions = {}
		} = {}
	} = rest as Partial<AMQPSubscriberOptions<M>>

	const { encode, decode } = serializer || V8Serializer
	const exchangesAsserted = new Set<string>()

	const dlxExchangeName = `${queueName}_dlx`
	const dlxExchangeBackToQueue = `${queueName}_dlx_back_to_queue`
	const dlxQueueName = `${queueName}_dlx_queue`

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
	let msgsBeingProcessed = 0

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
		__internal: { channel, publishNow: publish },
		...batcher,
		waitForOpen,
		async close() {
			// flush any pending events
			await batcher.flush()

			try {
				if(listenerTag) {
					await channel.cancel(listenerTag)
					logger.debug({ listenerTag }, 'cancelled listener')
					listenerTag = undefined

					while(msgsBeingProcessed > 0) {
						await new Promise(r => setTimeout(r, 100))
					}

					logger.debug('all msgs drained')
				}

				await channel.close()
				await conn.close()

				logger.info('closed event-bridge')
			} catch(err) {
				logger.error({ err }, 'error in closing event-bridge')
			}
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

		if(!queueName || !onEvent) {
			return
		}

		await startSubscription(channel)
	}

	async function startSubscription(channel: ConfirmChannel) {
		await channel.assertQueue(
			queueName,
			{
				...DEFAULT_QUEUE_OPTIONS,
				...queueOptions,
				deadLetterExchange: delayedRetrySeconds
					? dlxExchangeName
					: undefined,
				arguments: {
					...DEFAULT_QUEUE_OPTIONS.arguments,
					...queueOptions?.arguments,
					'x-delivery-limit': maxInitialRetries,
				}
			}
		)

		logger.debug({ queueName }, 'asserted queue')

		for(const event of events) {
			const exchange = String(event)
			await assertExchangeIfRequired(exchange, channel)
			await channel.bindQueue(queueName, exchange, '*')
		}

		logger.debug({ queueName, events }, 'bound queue to exchanges')

		await setupDelayedRetry(channel)

		// start the subscription
		const { consumerTag } = await channel.consume(
			queueName,
			consumerHandler,
			{ noAck: false }
		)

		listenerTag = consumerTag
		logger.debug({ consumerTag }, 'consuming events')
	}

	async function setupDelayedRetry(channel: ConfirmChannel) {
		if(!delayedRetrySeconds) {
			return
		}

		await channel.assertExchange(dlxExchangeName, 'fanout')
		await channel.assertExchange(dlxExchangeBackToQueue, 'fanout')
		await channel.assertQueue(
			dlxQueueName,
			{
				messageTtl: delayedRetrySeconds * 1000,
				deadLetterExchange: dlxExchangeBackToQueue,
				durable: true,
			}
		)
		await channel.bindQueue(dlxQueueName, dlxExchangeName, '')
		await channel.bindQueue(queueName, dlxExchangeBackToQueue, '')

		logger.info(
			{
				delayedRetrySeconds,
				dlxExchangeName,
				dlxQueueName,
				dlxExchangeBackToQueue
			},
			'setup DLX exchange'
		)
	}

	async function consumerHandler(msg: ConsumeMessage) {
		const exchange = msg.fields.exchange as E
		const msgId = msg.properties.messageId
		// owner ID is in the routing key
		const ownerId = msg.fields.routingKey
		const retryCount = +(
			msg.properties.headers?.['x-delivery-count'] || 0
		)
		const dlxRequeue = msg.properties.headers?.['x-death']
		const dlxRequeueCount = dlxRequeue?.[0]?.count

		const _logger = logger.child({
			exchange,
			ownerId,
			msgId,
			retryCount: retryCount || undefined,
			dlxRequeueCount,
		})

		let data: any
		try {
			msgsBeingProcessed += 1
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
			if(dlxRequeueCount && dlxRequeueCount >= maxDelayedRetries) {
				_logger.error(
					{ err },
					'error in handling msg. Final DLX retries exceeded.'
					+ ' Will not requeue'
				)
				channel.ack(msg)
				return
			}

			const retryNow = retryCount < maxInitialRetries
				&& !dlxRequeueCount
			const errMsg = retryNow
				? 'error in handling msg'
				: 'error in handling msg. Will retry later.'
			_logger.error({ retryNow, err }, errMsg)
			channel.nack(msg, undefined, retryNow)
		} finally {
			msgsBeingProcessed -= 1
		}
	}

	async function assertExchangeIfRequired(
		exchangeName: string,
		channel: ChannelWrapper | ConfirmChannel
	) {
		if(exchangesAsserted.has(exchangeName)) {
			return
		}

		// topic exchanges so we can match on routing key
		await channel.assertExchange(exchangeName, 'topic')
		exchangesAsserted.add(exchangeName)
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