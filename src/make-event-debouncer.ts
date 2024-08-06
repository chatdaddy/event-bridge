import type { EventDebouncerOptions } from "./types"

/**
 * Event debouncer batches events by event type and owner id,
 * and flushes them either at regular intervals if specified
 * or when the threshold is reached
 * or when asked for using the flush() function
 * @param options config options for the debouncer
 */
export default function makeEventDebouncer<M>({
	publish, logger,
	eventsPushIntervalMs, maxEventsForFlush
}: EventDebouncerOptions<M>) {
	logger = logger.child({ stream: 'events-manager' })

	/// total pending events to flush
	let eventCount = 0
	/// map of pending events
	let events: { [K in keyof M]?: { [ownerId: string]: M[K][] } } = { }
	/// regular flush interval
	let interval: NodeJS.Timeout | undefined = undefined

	/** push out all pending events */
	const flush = async() => {
		if(!eventCount) {
			return
		}

		logger.debug('flushing events...')
		const eventsToFlush = events
		const eventCountToFlush = eventCount
		events = {}
		eventCount = 0

		const eventsToFlushList = Object.keys(eventsToFlush) as (keyof M)[]

		await Promise.all(
			// flush all events
			eventsToFlushList.flatMap(
				event => (
					// flush all owner IDs
					Object.keys(eventsToFlush[event]!).map(
						async ownerId => {
							const events = eventsToFlush[event]![ownerId]
							try {
								await publish(event, events, ownerId)
							} catch(error) {
								logger.error(
									{
										trace: error.stack,
										length: events.length,
										event,
										ownerId
									},
									'error in emitting events'
								)
							}
						}
					)
				)
			)
		)
		logger.debug(`flushed ${eventCountToFlush} events`)
	}

	if(eventsPushIntervalMs) {
		logger.trace(
			{ ms: eventsPushIntervalMs },
			'starting regular flush...'
		)
		interval = setInterval(flush, eventsPushIntervalMs)
	}

	return {
		/**
		 * add pending event to the existing batch
		 * use flush() to publish immediately
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
			eventCount += 1

			if(eventCount > maxEventsForFlush) {
				flush()
			}
		},
		flush,
		close() {
			clearInterval(interval)
		}
	}
}