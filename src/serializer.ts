import { deserialize as V8Decode, serialize as V8Encode } from 'v8'
import type { Serializer } from './types'

export const V8Serializer: Serializer<any> = {
	encode: obj => {
		try {
			return V8Encode(obj)
		} catch(error) {
			obj = JSON.parse(JSON.stringify(obj))
			return V8Encode(obj)
		}
	},
	decode: buff => V8Decode(buff)
}