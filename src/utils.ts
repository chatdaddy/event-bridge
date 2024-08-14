import { randomBytes } from 'crypto'

// number of characters in the date part of the message ID
const dtLen = 10
// number of characters in the suffix part of the message ID
const suffixLen = 6
const msgPrefix = 'msg_'

/**
 * Creates a chronological unique message ID.
 */
export function makeUqMessageId(
	dt = new Date(),
	suffix = randomBytes(4).toString('base64url')
) {
	const dtStr = dt.getTime().toString(36).padStart(dtLen, '0')
	const str = `${dtStr}${suffix}`.slice(0, dtLen + suffixLen)
	return `${msgPrefix}${str}`
}

export function parseMessageId(id: string) {
	if(!id.startsWith(msgPrefix)) {
		return
	}

	const dtNum = id.slice(msgPrefix.length, msgPrefix.length + dtLen)
	const suffix = id.slice(msgPrefix.length + dtLen)
	const dt = new Date(parseInt(dtNum, 36))
	if(isNaN(dt.getTime())) {
		return
	}

	return { dt, suffix }
}