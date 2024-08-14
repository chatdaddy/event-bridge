import { makeUqMessageId, parseMessageId } from '../utils'

describe('Utils Tests', () => {

	it('should serialise & parse a message ID', () => {
		const date = new Date()
		const suffix = Math.random().toString()
		const id = makeUqMessageId(date, suffix)
		expect(id).toBeTruthy()

		const parsed = parseMessageId(id)
		expect(parsed).toBeTruthy()
		expect(parsed!.dt.getTime()).toBe(date.getTime())
		expect(parsed!.suffix).toBe(suffix.slice(0, parsed?.suffix.length))
	})
})