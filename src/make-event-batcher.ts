import type { EventBatcherOptions, EventData } from './types'
import { makeRandomMsgId } from './utils'

/**
 * Map to store pending events -- we store events by
 * event type and owner id.
 */
type PendingEventMap<M> = {
	[K in keyof M]?: { [ownerId: string]: M[K][] }
}

type PendingPublish<M, E extends keyof M> = EventData<M, E> & {
	tries: number
}

type PendingPublishMap<M> = { [key: string]: PendingPublish<M, keyof M> }

/**
 * Fn batches events by event type and owner id,
 * and flushes them either:
 * - at regular intervals if specified
 * - when the threshold is reached
 * - when asked for using the flush() function
 *
 * Will automatically keep retrying failed publishes. If an event fails
 * to publish -- no guarantee is made that the event will be published
 * in order. However, the event will be retried until it is published.
 *
 * @param options config options
 */
export default function makeEventBatcher<M>({
	publish, logger,
	eventsPushIntervalMs,
	maxEventsForFlush
}: EventBatcherOptions<M>) {
	logger = logger.child({ stream: 'events-manager' })

	/// published event count
	/// total pending events to flush
	let pendingEventCount = 0
	/// map of pending events
	let events: PendingEventMap<M> = { }
	/// regular flush interval
	let timeout: NodeJS.Timeout | undefined = undefined
	// store publishes that failed -- so they can be retried
	const failedPublishes: PendingPublishMap<M> = {}
	// create a promise chain to queue failed publishes
	// avoids simultaneous retries
	let failedPublishQueue = Promise.resolve()

	return {
		/**
		 * add pending event to the existing batch.
		 * Use flush() to publish immediately
		 * */
		publish<Event extends keyof M>(
			event: Event,
			data: M[Event],
			ownerId: string
		) {
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

			if(pendingEventCount > maxEventsForFlush) {
				flush()
			}
		},
		flush,
	}

	/** push out all pending events */
	async function flush() {
		await flushFailedMessages()

		if(!pendingEventCount) {
			return
		}

		logger.debug('flushing events...')

		const eventCountToFlush = pendingEventCount
		const pendingPublishes: PendingPublishMap<M> = { }
		for(const event in events) {
			for(const ownerId in events[event]!) {
				const id = makeRandomMsgId()
				pendingPublishes[id] = {
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

		const failed = await publishBatch(pendingPublishes)
		logger.debug(
			{
				total: eventCountToFlush,
				failed: Object.values(failed).length
			},
			'flushed events'
		)

		// add to failed publishes -- so can be retried
		Object.assign(failedPublishes, failed)
	}

	async function flushFailedMessages() {
		const failedCount = Object.keys(failedPublishes).length
		if(!failedCount) {
			return
		}

		failedPublishQueue = failedPublishQueue
			.then(() => publishBatch(failedPublishes))
			.then(failed => (
				logger.info(
					{
						total: failedCount,
						failed: Object.values(failed).length
					},
					'flushed failed events'
				)
			))
		await failedPublishQueue
	}

	/**
	 * Publishes a batch of events
	 * @returns map of failed publishes
	 */
	async function publishBatch(map: PendingPublishMap<M>) {
		await Promise.all(Object.entries(map).map(async([id, value]) => {
			try {
				await publish({ messageId: id, ...value })
				delete map[id]
			} catch(err) {
				const { data, ...meta } = value
				logger.error(
					{ err, length: data.length, ...meta },
					'error in publishing events'
				)

				value.tries += 1
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