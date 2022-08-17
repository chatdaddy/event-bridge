import type { Logger } from 'pino'

export type EventDebouncerOptions<M> = {
	publish: (event: keyof M, data: M[keyof M][], ownerId: string) => void
	logger: Logger
	/** regular flush interval */
	eventsPushIntervalMs?: number
	/** max events to take in before initiating a flush */
	maxEventsForFlush: number
}

export default function makeEventDebouncer<M>({ publish, logger, eventsPushIntervalMs, maxEventsForFlush }: EventDebouncerOptions<M>) {
	logger = logger.child({ stream: 'events-manager' })

	let eventCount = 0
	let events: { [K in keyof M]?: { [ownerId: string]: M[K][] } } = { }
	let interval: NodeJS.Timeout | undefined = undefined

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
		logger.trace({ ms: eventsPushIntervalMs }, 'starting regular flush...')
		interval = setInterval(flush, eventsPushIntervalMs)
	}

	return {
		publish<Event extends keyof M>(event: Event, data: M[Event], ownerId: string) {
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
			clearInterval(interval!)
		}
	}
}