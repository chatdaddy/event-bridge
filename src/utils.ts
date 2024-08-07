import { randomBytes } from 'crypto'

export function makeRandomMsgId() {
	return `msg_${randomBytes(4).toString('hex')}`
}