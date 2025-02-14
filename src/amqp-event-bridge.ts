import type { ChannelWrapper, Options } from 'amqp-connection-manager'
import AMQP from 'amqp-connection-manager'
import type { IAmqpConnectionManager } from 'amqp-connection-manager/dist/types/AmqpConnectionManager'
import type { PublishOptions } from 'amqp-connection-manager/dist/types/ChannelWrapper'
import type { ConfirmChannel, ConsumeMessage } from 'amqplib'
import P, { type Logger } from 'pino'
import { V8Serializer } from './serializer/v8'
import { makeEventBatcher } from './make-event-batcher'
import type { AMQPEventBridge, AMQPEventBridgeOptions, AMQPSubscription, EventData, OpenSubscription, Serializer } from './types'
import { makeUqMessageId, parseMessageId } from './utils'

const DEFAULT_PUBLISH_OPTIONS: PublishOptions = {
	contentType: 'application/octet-stream',
	persistent: true,
}

const EVENT_NAME_HEADER = 'x-event-name'
const OWNER_ID_HEADER = 'x-owner-id'

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

const DEFAULT_MAX_MSGS_PER_WORKER = 10

export function makeAmqpEventBridge<M>(
	{
		amqpUri,
		logger = P(),
		serializer = V8Serializer,
		publishOptions,
		batcherConfig,
		maxItemsToLog = 50,
		loggingMode,
		...rest
	}: AMQPEventBridgeOptions<M>
): AMQPEventBridge<M> {
	type E = keyof M
	const subscriptions: AMQPSubscription<M>[] = []
	if('queueName' in rest && 'events' in rest) {
		subscriptions.push(rest)
	} else if('subscriptions' in rest) {
		subscriptions.push(...rest.subscriptions as AMQPSubscription<M>[])
	}

	const { encode } = serializer
	const exchangesAsserted = new Set<string>()

	const batcher = makeEventBatcher<M>({
		flush: publish,
		logger,
		maxEventsForFlush: DEFAULT_MSGS_TO_FLUSH,
		...batcherConfig,
	})

	const conn = AMQP.connect([amqpUri], { })
	const openSubs = subscriptions
		// only open subscriptions that have a maxMessagesPerWorker
		// that is not 0
		.filter(sub => (
			typeof sub.maxMessagesPerWorker === 'undefined'
			|| sub.maxMessagesPerWorker > 0
		))
		.map(sub => (
			openSubscription(sub, {
				conn,
				maxItemsToLog,
				logger: subscriptions.length > 1
					? logger.child({ queueName: sub.queueName })
					: logger,
				loggingMode,
				decode: serializer.decode,
				assertExchangeIfRequired,
			})
		))
	const makeSeparatePublisher = !openSubs.length
	const pubChannel = makeSeparatePublisher
		? conn.createChannel({ name: 'publisher' })
		: openSubs[0].channel

	conn.on('disconnect', (arg) => {
		logger.error({ err: arg.err }, 'error in AMQP connection')
	})
	conn.on('connectFailed', (err) => {
		logger.error({ err }, 'AMQP connect failed')
	})
	conn.on('connect', () => {
		logger.info('connected to AMQP')
	})

	conn.on('blocked', reason => {
		logger.warn({ reason }, 'AMQP connection blocked')
	})

	conn.on('unblocked', () => {
		logger.info('AMQP connection unblocked')
	})

	return {
		__internal: {
			pubChannel,
			subscriptions: openSubs,
			publishNow: publish
		},
		publish(event, ownerId, data) {
			return batcher.add({ event, ownerId, data })
		},
		flush: batcher.flush,
		waitForOpen,
		async sendDirect({ event, data, ownerId, queueName }) {
			const eventStr = event.toString()
			const msgId = makeUqMessageId()
			await pubChannel.sendToQueue(
				queueName,
				encode(data, event),
				{
					...DEFAULT_PUBLISH_OPTIONS,
					...publishOptions,
					messageId: msgId,
					contentType: serializer.contentType,
					headers: {
						[EVENT_NAME_HEADER]: eventStr,
						[OWNER_ID_HEADER]: ownerId,
					}
				}
			)

			logger.trace(
				{ queueName, items: data.length, ownerId, msgId },
				'sent to queue'
			)

			return { msgId }
		},
		async close() {
			// flush any pending events
			await batcher.flush()

			for(const sub of openSubs) {
				await sub.close()
			}

			if(makeSeparatePublisher) {
				await pubChannel.close()
			}

			await conn.close()

			logger.info('closed event-bridge')
		},
	}

	async function waitForOpen() {
		await pubChannel.waitForConnect()
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
		await assertExchangeIfRequired(exchange, pubChannel)

		await pubChannel.publish(
			exchange,
			ownerId,
			encode(data, event),
			{
				messageId: messageId,
				...DEFAULT_PUBLISH_OPTIONS,
				...publishOptions,
				headers: {
					[EVENT_NAME_HEADER]: event,
					[OWNER_ID_HEADER]: ownerId,
				}
			}
		)

		logger.trace(
			{ exchange, items: data.length, ownerId, messageId },
			'published'
		)
	}
}

type SubscriptionCtx = {
	conn: IAmqpConnectionManager
	logger: Logger
	decode: Serializer<any>['decode']
	assertExchangeIfRequired(
		exchangeName: string,
		channel: ConfirmChannel
	): Promise<void>
	maxItemsToLog: number
	loggingMode?: 'all' | 'errors'
}

type QueuedEventData<M> = {
	[E in keyof M]: {
		data: M[E][]
		messageId: string | undefined
		retryCount: number
		resolve(): void
		reject(err: Error): void
	}
}

function openSubscription<M>(
	{
		queueName = '',
		events = [],
		onEvent,
		maxMessagesPerWorker = DEFAULT_MAX_MSGS_PER_WORKER,
		queueConfig: {
			maxInitialRetries = 2,
			delayedRetrySeconds = DEFAULT_RETRY_DELAY_S,
			maxDelayedRetries = 3,
			options: queueOptions = {}
		} = {},
		batchConsumeIntervalMs,
	}: AMQPSubscription<M>,
	{
		conn,
		logger,
		decode,
		assertExchangeIfRequired,
		maxItemsToLog,
		loggingMode = 'all',
	}: SubscriptionCtx
): OpenSubscription {
	type E = keyof M

	const dlxExchangeName = `${queueName}_dlx`
	const dlxExchangeBackToQueue = `${queueName}_dlx_back_to_queue`
	const dlxQueueName = `${queueName}_dlx_queue`

	const consumeBatcher = batchConsumeIntervalMs
		? makeEventBatcher<QueuedEventData<M>>({
			maxEventsForFlush: maxMessagesPerWorker,
			eventsPushIntervalMs: batchConsumeIntervalMs,
			flush: flushBatch,
			logger,
		})
		: undefined

	let listenerTag: string | undefined
	let msgsBeingProcessed = 0

	const channel = conn.createChannel({
		name: `subscriber-${queueName}`,
		setup: startSubscription
	})

	channel.on('error', err => {
		logger.error({ err }, 'error in channel')
	})

	channel.on('close', () => {
		logger.info('channel closed')
	})

	return {
		channel,
		async close() {
			try {
				if(listenerTag) {
					await channel.cancel(listenerTag)
					logger.debug({ listenerTag }, 'cancelled listener')
					listenerTag = undefined

					await consumeBatcher?.flush()

					while(msgsBeingProcessed > 0) {
						await new Promise(r => setTimeout(r, 100))
					}

					logger.debug('all msgs drained')
				}

				await channel.close()

				logger.info('closed subscription')
			} catch(err) {
				logger.error({ err }, 'error in closing subscription')
			}
		},
	}

	async function startSubscription(channel: ConfirmChannel) {
		logger.info('starting subscription')

		await channel.prefetch(maxMessagesPerWorker)

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
					'x-delivery-limit': maxInitialRetries || undefined,
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
		logger.info(
			{ queueName, maxMessagesPerWorker, consumerTag },
			'consuming events'
		)
	}

	async function setupDelayedRetry(channel: ConfirmChannel) {
		if(!delayedRetrySeconds || !maxDelayedRetries) {
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
		const exchange = msg.properties.headers?.[EVENT_NAME_HEADER]
			|| msg.fields.exchange as E
		const msgId = msg.properties.messageId
		// owner ID is in the routing key
		const ownerId = msg.properties.headers?.[OWNER_ID_HEADER]
			|| msg.fields.routingKey
		const deliveryCount = +(
			msg.properties.headers?.['x-delivery-count'] || 0
		)
		const dlxRequeue = msg.properties.headers?.['x-death']
		const dlxRequeueCount = dlxRequeue?.[0]?.count
		const retryCount = deliveryCount + (dlxRequeueCount || 0)

		const _logger = logger.child({
			exchange,
			ownerId,
			msgId,
			retryCount: retryCount || undefined,
			dlxRequeueCount,
		})

		let data: any[]
		try {
			msgsBeingProcessed += 1
			data = decode(msg.content, exchange)
			if(!Array.isArray(data)) {
				data = [data]
			}

			const parsed = parseMessageId(msgId)

			if(loggingMode === 'all') {
				_logger.info(
					{
						eventTs: parsed?.dt,
						data: data.slice(0, maxItemsToLog),
						totalItems: data.length,
					},
					'handling msg'
				)
			}

			if(consumeBatcher) {
				await new Promise<void>((resolve, reject) => {
					consumeBatcher.add({
						event: exchange,
						data: {
							data,
							messageId: msgId,
							retryCount,
							reject,
							resolve,
						},
						ownerId,
					})
				})
			} else {
				await onEvent({
					ownerId,
					msgId,
					logger: _logger,
					data,
					event: exchange,
					retryCount
				})

				if(loggingMode === 'all') {
					_logger.info('handled msg')
				}
			}

			channel.ack(msg)
		} catch(err) {
			if(
				(dlxRequeueCount && dlxRequeueCount >= maxDelayedRetries)
				|| (
					!maxDelayedRetries
					&& retryCount >= maxInitialRetries
				)
			) {
				_logger.error(
					{
						err,
						data: loggingMode === 'errors'
							? data!?.slice(0, maxItemsToLog)
							: undefined
					},
					'error in handling msg. Final retries exceeded.'
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

	async function flushBatch({
		data,
		event,
		ownerId
	}: EventData<QueuedEventData<M>, keyof M>) {

		let maxRetryCount = 0
		const msgIds: string[] = []
		for(const { messageId, retryCount } of data) {
			if(messageId) {
				msgIds.push(messageId)
			}

			maxRetryCount = Math.max(maxRetryCount, retryCount)
		}

		const msgId = msgIds.length ? msgIds.join(' ') : undefined

		try {
			const _logger = logger.child({ event, ownerId, msgId })
			_logger.debug('processing batch')

			await onEvent({
				event,
				ownerId,
				data: data.flatMap(d => d.data),
				logger: _logger,
				msgId,
				retryCount: maxRetryCount,
			})
			for(const { resolve } of data) {
				resolve()
			}

			if(loggingMode === 'all') {
				_logger.info('handled msgs')
			}
		} catch(err) {
			for(const { reject } of data) {
				reject(err)
			}
		}
	}
}