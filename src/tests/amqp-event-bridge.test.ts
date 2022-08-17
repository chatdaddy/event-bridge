import dotenv from 'dotenv'
dotenv.config({ path: '.env.test' })

import { randomBytes } from 'crypto'
import P from 'pino'
import { AMQPEventBridge, makeAmqpEventBridge } from '../amqp-event-bridge'

type TestEventMap = {
	'my-cool-event': { value: number }
	'another-cool-event': { text: string }
}

const MAX_MESSAGES_PER_WORKER = 2

describe('AMQP Event Bridge Tests', () => {

	let connections: AMQPEventBridge<TestEventMap>[] = []

	beforeAll(async() => {
		connections = await Promise.all(
			[...Array(2)].map(
				async(_, i) => {
					const conn = makeAmqpEventBridge({
						amqpUri: process.env.AMQP_URI!,
						maxMessagesPerWorker: MAX_MESSAGES_PER_WORKER,
						maxEventsForFlush: 250,
						logger: P({ level: 'trace' }).child({ conn: i })
					})

					await conn.waitForOpen()
					return conn
				}
			)
		)
	})

	afterAll(async() => {
		await Promise.all(connections.map(c => c.close()))
	})

	it('should receive an event exactly once', async() => {
		let recvCount = 0

		const expectedOwnerId = '1234'
		const subId = 'cool-queue'
		const cancellations = await Promise.all(
			connections.map(
				conn => conn.subscribe({
					event: 'my-cool-event',
					subscriptionId: subId,
					listener: async(data, ownerId) => {
						expect(data).toHaveLength(1)
						expect(data[0].value).toBeGreaterThan(0)
						// only increment the recvCount if the ownerId matches the expectedOwnerId
						if(expectedOwnerId === ownerId) {
							recvCount += 1
						}
					}
				})
			)
		)

		const publishConn = connections[0]
		publishConn.publish('my-cool-event', { value: 10 }, expectedOwnerId)
		publishConn.publish('another-cool-event', { text: '123' }, expectedOwnerId)
		await publishConn.flush()

		await delay(100)

		await Promise.all(
			cancellations.map(c => c(true))
		)

		expect(recvCount).toBe(1)
	})

	it('should re-subscribe successfully', async() => {
		const conn = connections[0]
		const subscriptionId = 'my-cool-queue'
		for(let i = 0;i < 3;i++) {
			let eventsHandled = 0
			const cancel = await conn.subscribe({
				event: 'my-cool-event',
				subscriptionId,
				listener: async() => {
					eventsHandled += 1
				}
			})

			connections[1].publish('my-cool-event', { value: 10 }, i.toString())
			await connections[1].flush()

			await delay(100)

			await cancel(true)
			expect(eventsHandled).toBe(1)
		}
	})

	it('should not receive more than expected concurrent events', async() => {
		const conn = connections[0]

		let concurrentHandling = 0
		let eventsHandled = 0
		let didFail = false
		const cancel = await conn.subscribe({
			event: 'my-cool-event',
			listener: async() => {
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
			connections[1].publish('my-cool-event', { value: 10 }, i.toString())
		}

		await connections[1].flush()

		while(eventsHandled < total) {
			await delay(100)
		}

		await cancel()
		expect(didFail).toBe(false)
	})

	it('should handle multiple types of events', async() => {
		const conn = connections[0]
		const events = ['my-cool-event', 'another-cool-event'] as const
		const eventsRecvSet = new Set<typeof events[number]>()
		const expectedOwnerId = randomBytes(2).toString('hex')

		const cancellations = await Promise.all(
			events.map(event => conn.subscribe({
				event: event,
				subscriptionId: 'my-cool-queue',
				listener: async(_, ownerId) => {
					if(ownerId === expectedOwnerId) {
						eventsRecvSet.add(event)
					}
				}
			}))
		)

		const publishConn = connections[1]
		publishConn.publish('my-cool-event', { value: 10 }, expectedOwnerId)
		publishConn.publish('another-cool-event', { text: '123' }, expectedOwnerId)
		await publishConn.flush()

		await delay(100)

		await Promise.all(
			cancellations.map(c => c(true))
		)

		expect(eventsRecvSet.size).toBe(events.length)
	})

	it('should only receive messages for a specific owner', async() => {
		const conn = connections[0]
		const expectedOwnerId = randomBytes(2).toString('hex')
		const otherOwnerId = randomBytes(2).toString('hex')

		let eventsRecv = 0
		const cancel = await conn.subscribe({
			event: 'another-cool-event',
			ownerId: expectedOwnerId,
			listener: async(_, ownerId) => {
				if(ownerId === expectedOwnerId) {
					eventsRecv += 1
				}
			}
		})

		const publishConn = connections[1]
		publishConn.publish('another-cool-event', { text: 'abc' }, expectedOwnerId)
		publishConn.publish('another-cool-event', { text: '123' }, otherOwnerId)
		await publishConn.flush()

		await delay(100)

		await cancel(true)

		expect(eventsRecv).toBe(1)
	})

	it('should batch events for the same owner', async() => {
		const conn = connections[0]
		const expectedOwnerId = randomBytes(2).toString('hex')
		const eventCount = 5

		let eventsRecv = 0
		const cancel = await conn.subscribe({
			event: 'another-cool-event',
			ownerId: expectedOwnerId,
			subscriptionId: 'my-cool-queue',
			listener: async(data, ownerId) => {
				if(ownerId === expectedOwnerId && data.length === eventCount) {
					eventsRecv += 1
				}
			}
		})

		for(let i = 0;i < eventCount;i++) {
			connections[1].publish('another-cool-event', { text: 'abc ' + i }, expectedOwnerId)
		}

		await connections[1].flush()

		await delay(100)

		await cancel(true)

		// events should have batched
		// and only a single event published
		expect(eventsRecv).toEqual(1)
	})

	it('should not receive events after cancellation', async() => {
		const conn = connections[0]

		let didFail = false
		const cancel1 = await conn.subscribe({
			event: 'another-cool-event',
			subscriptionId: 'cool-queue-2',
			listener: async() => {
				didFail = true
			}
		})

		const cancel2 = await conn.subscribe({
			event: 'my-cool-event',
			subscriptionId: 'my-cool-queue',
			listener: async() => { }
		})

		await cancel1(true)

		connections[1].publish('another-cool-event', { text: 'abc' }, 'abcd')
		connections[1].publish('my-cool-event', { value: 123 }, 'abcd')
		await connections[1].flush()

		await cancel2(true)

		expect(didFail).toBeFalsy()
	})
})

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))