import type { EventBatcherOptions, EventData } from './types'
import { makeUqMessageId } from './utils'

/**
 * Map to store pending events -- we store events by
 * event type and owner id.
 */
type PendingEventMap<M> = {
	[K in keyof M]?: { [ownerId: string]: M[K][] }
}

type PendingFlush<M, E extends keyof M> = EventData<M, E> & {
	tries: number
}

type PendingFlushMap<M> = { [key: string]: PendingFlush<M, keyof M> }

type AddToBatchOpts<M, E extends keyof M> = {
	event: E
	ownerId: string
	data: M[E]
}

/**
 * Fn batches events by event type and owner id,
 * and flushes them either:
 * - at regular intervals if specified
 * - when the threshold is reached
 * - when asked for using the flush() function
 *
 * Will automatically keep retrying failed flushes. If an event fails
 * to flush -- no guarantee is made that the event will be flushed
 * in order. However, the event will be retried until it is flushed.
 *
 * @param options config options
 */
export function makeEventBatcher<M>({
	flush: _flush, logger,
	eventsPushIntervalMs,
	maxEventsForFlush,
	maxRetries = 3
}: EventBatcherOptions<M>) {
	logger = logger.child({ stream: 'events-manager' })

	/// flushed event count
	/// total pending events to flush
	let pendingEventCount = 0
	/// map of pending events
	let events: PendingEventMap<M> = { }
	/// regular flush interval
	let timeout: NodeJS.Timeout | undefined = undefined
	// store flushes that failed -- so they can be retried
	const failedFlushes: PendingFlushMap<M> = {}
	// create a promise chain to queue failed flushes
	// avoids simultaneous retries
	let failedQueue = Promise.resolve()

	return {
		/**
		 * @deprecated use add() instead
		 */
		publish<Event extends keyof M>(
			event: Event,
			ownerId: string,
			data: M[Event]
		) {
			return add<Event>({ event, ownerId, data })
		},
		/**
		 * add pending event to the existing batch.
		 * Use flush() to flush immediately
		 * */
		add,
		flush,
	}

	function add<Event extends keyof M>({
		event,
		ownerId,
		data,
	}: AddToBatchOpts<M, Event>) {
		let map = events[event]
		if(!events[event]) {
			map = { }
			events[event] = map
		}

		if(!map![ownerId]) {
			(map as any)[ownerId] = []
		}

		map![ownerId].push({ ...data })
		pendingEventCount += 1

		startTimeout()

		if(pendingEventCount >= maxEventsForFlush) {
			flush()
		}
	}

	/** push out all pending events */
	function flush() {
		logger.debug({ pendingEventCount }, 'flushing events...')

		const eventCountToFlush = pendingEventCount
		const pending: PendingFlushMap<M> = { }
		for(const event in events) {
			for(const ownerId in events[event]!) {
				const id = makeUqMessageId()
				pending[id] = {
					data: events[event]![ownerId],
					event: event as keyof M,
					ownerId,
					tries: 0
				}
			}
		}

		events = {}
		pendingEventCount = 0
		// remove timeout -- as we'll flush now
		// and start a new one if needed
		clearTimeout(timeout)
		timeout = undefined

		return postFlush(eventCountToFlush, pending)
	}

	async function postFlush(eventCount: number, batch: PendingFlushMap<M>) {
		await flushFailedMessages()

		if(!eventCount) {
			return
		}

		const failed = await flushBatch(batch)
		logger.debug(
			{
				total: eventCount,
				failed: Object.values(failed).length
			},
			'flushed events'
		)

		// add to failed flushes -- so can be retried
		Object.assign(failedFlushes, failed)
	}

	async function flushFailedMessages() {
		const failedCount = Object.keys(failedFlushes).length
		if(!failedCount) {
			return
		}

		failedQueue = failedQueue
			.then(() => flushBatch(failedFlushes))
			.then(failed => (
				logger.info(
					{
						total: failedCount,
						failed: Object.values(failed).length
					},
					'flushed failed events'
				)
			))
		await failedQueue
	}

	/**
	 * Flushes a batch of events
	 * @returns map of failed flushes
	 */
	async function flushBatch(map: PendingFlushMap<M>) {
		await Promise.all(Object.entries(map).map(async([msgId, value]) => {
			try {
				await _flush({ messageId: msgId, ...value })
				delete map[msgId]
			} catch(err) {
				const { data, ...meta } = value

				if(value.tries >= maxRetries) {
					logger.error(
						{ err, msgId, ...value },
						'failed to flush event after retries'
					)
				} else {
					logger.error(
						{ err, length: data.length, msgId, ...meta },
						'error in flushing events'
					)

					value.tries += 1
				}
			}
		}))

		return map
	}

	function startTimeout() {
		if(timeout) {
			return
		}

		if(!eventsPushIntervalMs) {
			return
		}

		logger.trace(
			{ ms: eventsPushIntervalMs },
			'scheduled flush...'
		)
		timeout = setTimeout(flush, eventsPushIntervalMs)
	}
}