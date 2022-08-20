import { deserialize as V8Decode, serialize as V8Encode } from 'v8'

export type Serializer<Event> = {
    encode: (obj: any, event: Event) => Buffer
    decode: (enc: Buffer, event: Event) => any
}

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