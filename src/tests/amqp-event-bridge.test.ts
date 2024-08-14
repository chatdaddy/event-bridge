import dotenv from 'dotenv'
dotenv.config({ path: '.env.test' })

import { randomBytes } from 'crypto'
import P from 'pino'
import { makeAmqpEventBridge } from '../amqp-event-bridge'
import { AMQPEventBridge, AMQPEventBridgeOptions } from '../types'

type TestEventMap = {
	'my-cool-event': { value: number }
	'another-cool-event': { text: string }
}

const MAX_MESSAGES_PER_WORKER = 2
const MAX_MSGS_BEFORE_FLUSH = 20
const MAX_MESSAGE_RETRIES = 3
const EVENT_FLUSH_INTERVAL_MS = 200
const DELAY_RETRY_S = 2
const MAX_DELAYED_RETRIES = 3
const LOGGER = P({ level: 'trace' })

describe('AMQP Event Bridge Tests', () => {

	let workerId: string
	let connections: AMQPEventBridge<TestEventMap>[] = []
	let publisher: AMQPEventBridge<TestEventMap>

	beforeEach(async() => {
		workerId = `wrk_${randomBytes(2).toString('hex')}`
		connections = []
		publisher = await openConnection({
			amqpUri: process.env.AMQP_URI!,
			logger: LOGGER.child({ conn: 'publisher' }),
		})
	})

	afterEach(async() => {
		await Promise.all(connections.map(c => c.close()))
	})

	it('should not be consuming events before subscription', async() => {
		const queue = await publisher.__internal.channel
			.checkQueue(workerId)
			.catch(() => undefined)
		expect(queue?.consumerCount).toBeFalsy()
	})

	it('should receive an event exactly once', async() => {
		let recvCount = 0

		const expectedOwnerId = '1234'
		const expectedEvent: keyof TestEventMap = 'my-cool-event'

		await Promise.all(
			[...Array(2)].map(
				(_, i) => openConnection({
					logger: LOGGER.child({ conn: 'subscriber-' + i }),
					onEvent: async({ event, data }) => {
						expect(event).toEqual(expectedEvent)
						expect(data).toHaveLength(1)
						if(event !== 'my-cool-event') {
							fail('Unexpected event')
						}

						expect(data[0].value).toBeGreaterThan(0)
						recvCount += 1
					}
				})
			)
		)

		publisher.publish(expectedEvent, { value: 10 }, expectedOwnerId)
		await publisher.flush()

		await delay(200)

		expect(recvCount).toBe(1)
	})

	it('should automatically publish once interval is reached', async() => {
		let recvCount = 0

		const expectedOwnerId = '1234567'
		const expectedEvent: keyof TestEventMap = 'my-cool-event'

		await openConnection({
			onEvent: async({ ownerId }) => {
				if(ownerId === expectedOwnerId) {
					recvCount += 1
				}
			}
		})

		publisher.publish(expectedEvent, { value: 1 }, expectedOwnerId)
		publisher.publish(expectedEvent, { value: 2 }, '')
		await delay(500)
		expect(recvCount).toBe(1)
	})

	it('should receive message if consumer started after message', async() => {
		let recvCount = 0
		// just ensure queue exists to store the msg
		const conn = await openConnection({
			onEvent: async() => {
				recvCount += 1
			}
		})
		await conn.close()

		publisher.publish('my-cool-event', { value: 10 }, '123')
		await publisher.flush()

		await openConnection({
			onEvent: async() => {
				recvCount += 1
			}
		})

		await delay(200)
		expect(recvCount).toBe(1)
	})

	it('should expire message after ttl', async() => {
		const ttlSeconds = 1
		let conn = await openConnection({
			onEvent: async() => {
				recvCount += 1
			},
			queueConfig: {
				delayedRetrySeconds: 0,
				options: {
					messageTtl: ttlSeconds,
				}
			},
		})
		await conn.close()

		publisher.publish('my-cool-event', { value: 10 }, '123')
		await publisher.flush()

		await delay(ttlSeconds + 50)

		let recvCount = 0
		conn = await openConnection({
			onEvent: async() => {
				recvCount += 1
			},
			queueConfig: {
				delayedRetrySeconds: 0,
				options: {
					messageTtl: ttlSeconds,
				}
			},
		})

		await delay(100)
		expect(recvCount).toBe(0)
	})

	it('should retry publish failures', async() => {
		const event: keyof TestEventMap = 'my-cool-event'
		const ownerId = '123123123123'

		let recvCount = 0

		await openConnection({
			onEvent: async({ ownerId: recvOwnerId }) => {
				if(ownerId === recvOwnerId) {
					recvCount += 1
				}
			}
		})

		const channel = publisher.__internal.channel
		const publishMock = jest.spyOn(channel, 'publish')
		publishMock.mockImplementationOnce(() => {
			throw new Error('Test error')
		})

		publisher.publish(event, { value: 10 }, ownerId)
		await publisher.flush()

		expect(recvCount).toBe(0)

		expect(publishMock).toHaveBeenCalledTimes(1)

		await publisher.flush()
		expect(recvCount).toBe(1)

		// same msg ID should be used for retries
		const msgIdSet = new Set(
			publishMock.mock.calls.map(c => c[3]?.messageId)
		)
		expect(msgIdSet.size).toBe(1)
	})

	it('should still listen after reconnection', async() => {
		let eventRecv = 0
		const conn = await openConnection({
			onEvent: async() => {
				eventRecv += 1
			}
		})

		const rawConn = conn.__internal.channel['_connectionManager']
		rawConn.reconnect()

		publisher.publish('my-cool-event', { value: 10 }, '123')
		await publisher.flush()

		await delay(100)

		expect(eventRecv).toBe(1)
	})

	it('should keep retrying msg consumption till success', async() => {
		let tries = 0
		await openConnection({
			onEvent: async() => {
				tries += 1
				if(tries < 3) {
					throw new Error('Test error')
				}
			}
		})

		publisher.publish('my-cool-event', { value: 10 }, '123')
		await publisher.flush()

		await delay(100)
		expect(tries).toBe(3)
	})

	it('should not deliver msg after retry limit', async() => {
		let tries = 0
		await openConnection({
			onEvent: async() => {
				tries += 1
				throw new Error('Test error')
			},
			queueConfig: {
				delayedRetrySeconds: 0
			}
		})

		publisher.publish('my-cool-event', { value: 10 }, '123')
		await publisher.flush()

		await delay(200)
		expect(tries).toBe(MAX_MESSAGE_RETRIES + 1)
	})

	it('should requeue after delay', async() => {
		const expOwnerId = '123'
		let tries = 0
		await openConnection({
			onEvent: async({ ownerId }) => {
				expect(ownerId).toBe(expOwnerId)
				tries += 1
				throw new Error('Test error')
			},
		})

		publisher.publish('my-cool-event', { value: 10 }, expOwnerId)
		await publisher.flush()

		await delay(200)
		expect(tries).toBe(MAX_MESSAGE_RETRIES + 1)

		// check if it retries after delay -- and stops after max retries
		for(let i = 0;i < MAX_DELAYED_RETRIES;i++) {
			await delay(DELAY_RETRY_S * 1000 + 100)
			expect(tries).toBe(MAX_MESSAGE_RETRIES + 1 + (i + 1))
		}

		// ensure no new messages are received
		await delay(DELAY_RETRY_S * 1000 + 100)
		expect(tries).toBe(MAX_MESSAGE_RETRIES + 1 + MAX_DELAYED_RETRIES)
	}, 20_000)

	it('should not receive more than expected concurrent events', async() => {
		let concurrentHandling = 0
		let eventsHandled = 0
		let didFail = false
		await openConnection({
			onEvent: async() => {
				concurrentHandling += 1
				eventsHandled += 1

				if(concurrentHandling > MAX_MESSAGES_PER_WORKER) {
					didFail = true
				}

				await delay(100)

				concurrentHandling -= 1
			}
		})

		const total = 4
		for(let i = 0;i < total;i++) {
			publisher.publish(
				'my-cool-event',
				{ value: 10 },
				i.toString()
			)
		}

		await publisher.flush()

		while(eventsHandled < total) {
			await delay(100)
		}

		expect(didFail).toBe(false)
	})

	it('should handle multiple types of events', async() => {
		const events = ['my-cool-event', 'another-cool-event'] as const
		const eventsRecvSet = new Set<typeof events[number]>()
		const expectedOwnerId = randomBytes(2).toString('hex')

		await openConnection({
			onEvent: async({ ownerId, event }) => {
				if(ownerId === expectedOwnerId) {
					eventsRecvSet.add(event)
				}
			}
		})

		publisher.publish('my-cool-event', { value: 10 }, expectedOwnerId)
		publisher.publish('another-cool-event', { text: '123' }, expectedOwnerId)
		await publisher.flush()

		await delay(200)

		expect(eventsRecvSet.size).toBe(events.length)
	})

	it('should batch events for the same owner', async() => {
		const expectedOwnerId = randomBytes(2).toString('hex')
		const eventCount = 5

		let eventsRecv = 0
		await openConnection({
			onEvent: async({ data, ownerId }) => {
				if(ownerId === expectedOwnerId && data.length === eventCount) {
					eventsRecv += 1
				}
			}
		})

		for(let i = 0;i < eventCount;i++) {
			publisher.publish('another-cool-event', { text: 'abc ' + i }, expectedOwnerId)
		}

		await publisher.flush()

		await delay(100)
		// events should have batched
		// and only a single event published
		expect(eventsRecv).toEqual(1)
	})

	it('should flush events after max events reached', async() => {
		const expectedOwnerId = randomBytes(2).toString('hex')
		const eventCount = MAX_MSGS_BEFORE_FLUSH + 1

		let dataRecv = 0
		await openConnection({
			onEvent: async({ data, ownerId }) => {
				if(ownerId !== expectedOwnerId) {
					return
				}

				dataRecv += data.length
			}
		})

		for(let i = 0;i < eventCount;i++) {
			publisher.publish(
				'another-cool-event',
				{ text: 'abc ' + i },
				expectedOwnerId
			)
		}

		await delay(200)
		expect(dataRecv).toEqual(eventCount)
	})

	async function openConnection(
		opts: Partial<AMQPEventBridgeOptions<TestEventMap>>,
	) {
		if('onEvent' in opts) {
			opts = {
				workerId,
				events: ['my-cool-event', 'another-cool-event'],
				...opts,
				queueConfig: {
					maxInitialRetries: MAX_MESSAGE_RETRIES,
					delayedRetrySeconds: DELAY_RETRY_S,
					maxDelayedRetries: MAX_DELAYED_RETRIES,
					...opts.queueConfig,
				},
			}
		}

		const conn = makeAmqpEventBridge({
			amqpUri: process.env.AMQP_URI!,
			maxMessagesPerWorker: MAX_MESSAGES_PER_WORKER,
			logger: LOGGER,
			batcherConfig: {
				maxEventsForFlush: MAX_MSGS_BEFORE_FLUSH,
				eventsPushIntervalMs: EVENT_FLUSH_INTERVAL_MS
			},
			...opts,
		})

		connections.push(conn)

		await conn.waitForOpen()
		return conn
	}
})

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))