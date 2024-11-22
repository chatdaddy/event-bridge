import type { Serializer } from '../types'

export const JSONSerializer: Serializer<any> = {
	encode: obj => Buffer.from(JSON.stringify(obj)),
	decode: buff => JSON.parse(buff.toString()),
	contentType: 'application/json'
}