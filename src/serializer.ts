import { deserialize as V8Decode, serialize as V8Encode } from 'v8'

export type Serializer<T extends string | Buffer> = {
    encode: (obj: any) => T
    decode: (enc: T) => any
}

export const V8Serializer: Serializer<Buffer> = {
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