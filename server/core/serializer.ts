import * as crypto from "crypto";
import {Err, Ok, type Result} from "../utils/result";
import {DeserializeError} from "../errors/deser-error";

class Encryptor {
    constructor() {}
    protected encrypt(dataToEncrypt: string, key: string) {
        const keyBuf = Buffer.from(key);
        const iv = crypto.randomBytes(16);

        const encryptionContext = crypto.createCipheriv('aes-256-cbc', keyBuf, iv);
        const firstHalf = encryptionContext.update(Buffer.from(dataToEncrypt));
        const lastHalf = Buffer.concat([firstHalf, encryptionContext.final()]);
        return Buffer.concat([iv, lastHalf]).toString('hex');
    }

    protected decrypt(dataToDecrypt: string, key: string) {
        const keyBuf = Buffer.from(key);
        const convertedData = Buffer.from(dataToDecrypt, 'hex');

        const iv = convertedData.slice(0, 16);
        const buf = convertedData.slice(16);

        const decryptionContext = crypto.createDecipheriv(
            'aes-256-cbc',
            keyBuf,
            iv,
        );
        const firstPart = decryptionContext.update(buf);
        return Buffer.concat([firstPart, decryptionContext.final()]).toString(
            'binary',
        );
    }
}

type PrimitiveTys = "bool" | "int" | "float" | "str"
function resolveTy(val: string | number | boolean) {
    switch (typeof val) {
        case "boolean":
            return "bool"
        case "number": {
            if (Number.isInteger(val)) {
                return "int"
            }
            return "float"
        }
        default:
            return "str"
    }
}

function castType(val: string, tty: PrimitiveTys) {
    switch (tty) {
        case "bool":
            return Boolean(val)
        case "float":
            return parseFloat(val)
        case "int":
            return parseInt(val)
        case "str":
            return val
    }
}
interface PrimitiveValue {
    ___INT_SINGULAR_VAL: true,
    ___INT_ACTUAL_VALUE: any,
    ___INT_CAST_INTO: PrimitiveTys
}

function createSingularValue(val: string | number | boolean): PrimitiveValue {
    return {
        ___INT_SINGULAR_VAL: true,
        ___INT_ACTUAL_VALUE: val,
        ___INT_CAST_INTO: resolveTy(val)
    }
}

function isSingularValue(obj: PrimitiveValue | any): obj is PrimitiveValue {
    return (obj.___INT_SINGULAR_VAL && obj.___INT_CAST_INTO && obj.___INT_ACTUAL_VALUE)
}

export class Serializer extends Encryptor {
    constructor() {
        super()
    }

    public serialize<T extends Object | number | string | boolean>(obj: T): string {
        if (typeof obj === "object") {
            return JSON.stringify(obj)
        } else {
            return JSON.stringify(createSingularValue(obj))
        }
    }

    public deserialize<T extends any>(data: string): Result<T, DeserializeError> {
        if (isSingularValue(data)) {
            const val: PrimitiveValue = JSON.parse(data)
            //@ts-ignore
            return Ok(castType(val.___INT_ACTUAL_VALUE, val.___INT_CAST_INTO))
        }
        try {
            const res = JSON.parse(data)
            return Ok(res)
        } catch (err) {
            return Err(new DeserializeError(data, err))
        }
    }

    protected serializeAndEncrypt<T>(data: T, key: string) {
            return this.encrypt(this.serialize(data), key)
    }

    protected deserializeAndDecrypt(data: string, key: string) {
        return this.deserialize(this.decrypt(data, key))
    }
}



