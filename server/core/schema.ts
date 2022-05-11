import type {RootSchema, SimplifySchema, SubSchema, ValidPrimitives, ValidSchemas} from "./database";
import {
    KIND_MULTI_ARRAY,
    KIND_PRIMITIVE,
    KIND_PRIMITIVE_ARRAY,
    KIND_ROOT,
    KIND_SUB,
    KIND_SUB_ARRAY,
    type SchemaRtKinds
} from "./constants";
import {Err, Ok, Result} from "../utils/result";
import {Const, RequiredPrimitives} from "./database";

//runtime schema utilities

//cool functional programming functions
const ALL_VALID_PRIMS = ["str", "bool", "int", "float", "float?", "str?", "int?", "bool?"]
const ALL_VALID_NOOPT_PRIMS = ["str", "bool", "int", "float"]

const isPrimitiveValue = (val: any) => ["string", "boolean", "number"].includes(typeof val)
const IsValidPrimitiveOpt = (val: any) => ALL_VALID_PRIMS.includes(val)
const isValidPrimitive = (val: any, allowOpt = false) => allowOpt ? IsValidPrimitiveOpt(val) : ALL_VALID_NOOPT_PRIMS.includes(val)
const isKind = (val: any, kinds: SchemaRtKinds | SchemaRtKinds[]) => (Array.isArray(kinds) ? kinds : [kinds]).includes(val) //array conversion required due to mixed strings

const resolvePrimitiveType = (val: any): RequiredPrimitives => {
    if (typeof val === "number" && !Number.isInteger(val)) {
        return "float"
    }
    switch (typeof val) {
        case "string":
            return "str"
        case "number":
            return "int"
        case "boolean":
            return "bool"
    }
}



const objectArrayAllElementsPass = <T extends Record<string, any>>(obj: T, func: (key: keyof T, val: T[keyof T]) => boolean) => {
    if (typeof obj !== "object") {
        return false
    }
    const ents = Object.entries(obj)
    for (const [k, v] of ents) {
        if (!func(k, v)) {
            return false
        }
    }
    return true

}

const collectKeysWhere = <T extends Record<string, any>>(obj: T, func: (key: keyof T, val: T[keyof T]) => boolean): (keyof T)[] => {
    if (typeof obj !== "object") {
        return []
    }
    const ents = Object.entries(obj)
    const res: (keyof T)[] = []
    for (const [k, v] of ents) {
        if (func(k, v)) {
            res.push(k)
        }
    }
    return res
}

const isOptional = (cand: string) => cand.endsWith("?")

const equals = (cand1: ValidPrimitives, cand2: ValidPrimitives, ignoreOptionalFlag = true) => {
    if (ignoreOptionalFlag) {
        return cand1.replace("?", "") === cand2.replace("?", "")
    }
    return cand2 === cand1
}

//weird magic string
const FAIL_MARKER = "____+!+=!T+Q/FDA/FAILED|||OMGOMG" + Date.now().toString()

const _safeAccess = (p: string[], o: Record<string, any>) => p.reduce((obj, ck) => (obj && obj[ck]) ? obj[ck] : FAIL_MARKER, o);

const safeAccess = <T>(obj: Record<string, any>, path: string[]): Result<T, null> => {
    const res = _safeAccess(path, obj)
    if (FAIL_MARKER === res) {
        return Err(null)
    }
    return Ok(res)
}




export class SchemaManager<T extends ValidSchemas> {
    private readonly kind: SchemaRtKinds
    //functions prefixed with "rt" are only called when the doStrictRtTypeChecks param is set to true (required when using non-typed environments)
    constructor(private readonly schema: T, private readonly doStrictRtTypeChecks = false) {
        this.kind = this.getKind(schema, true)
        console.log("Schema kind", this.kind)
        if (this.doStrictRtTypeChecks) {
            console.log("Validating schema")
            this.rtValidateSchema()
            console.log("Schema validated!")
        }
    }
    private getKind<B extends boolean>(schema: ValidSchemas | SubSchema | { pointer: boolean, type: any}, throwErr?: B, key?: string): B extends true ? SchemaRtKinds : SchemaRtKinds | boolean {
        if (Array.isArray(schema)) {
            const containsPrimitive = schema.some(val => isPrimitiveValue(val))
            const containsNonPrimitive = schema.some(val => !isPrimitiveValue(val))
            return containsNonPrimitive && containsPrimitive ? KIND_MULTI_ARRAY : containsNonPrimitive ? KIND_SUB_ARRAY : KIND_PRIMITIVE_ARRAY
        }
        if (isValidPrimitive(schema, true)) {
            return KIND_PRIMITIVE
        }
        if (typeof schema === "object" && objectArrayAllElementsPass(schema, (_, sch) => (<any>sch)?.pointer !== null && (<any>sch)?.type !== null)) {
            return KIND_ROOT
        }
        if (typeof schema === "object") {
            return KIND_SUB
        }

        if (throwErr) {
            throw new Error(`Invalid schema was received (key: ${key}), this is only possible when you either used an untyped Table class, or overrode the type system. (received: ${JSON.stringify(schema, null, 2)})`)
        } else {
            return false as  B extends true ? SchemaRtKinds : SchemaRtKinds | boolean
        }
    }

    private rtValidateSchema(kind: SchemaRtKinds = this.kind, schema: ValidSchemas | SubSchema | { pointer: boolean, type: any} = this.schema, key?: string) {
        console.log("rtValidateSchema", kind, schema)
        if (isKind(kind, KIND_MULTI_ARRAY)) {
            return this.rtValidateMixArraySchema(schema as (ValidPrimitives |  SubSchema)[])
        }
        if (isKind(kind, KIND_SUB_ARRAY)) {
            return (<Array<SubSchema>>schema).forEach(this.rtValidateSubSchema.bind(this))
        }
        if (isKind(kind, KIND_PRIMITIVE_ARRAY)) {
            if (!(<Array<ValidPrimitives>>schema).every((val) => isValidPrimitive(val, true))) {
                throw new Error(`[RT-SCHEMA-CHECK] Invalid primitive array schema (key: ${key}). (received: ${JSON.stringify(schema)}, kind: ${kind}), expected one of these: ${JSON.stringify(ALL_VALID_PRIMS)}`)
            }
        }
        if (isKind(kind, KIND_PRIMITIVE)) {
            if (!isValidPrimitive(schema, true)) {
                throw new Error(`[RT-SCHEMA-CHECK] Invalid primitive type schema (key: ${key}). (received: ${JSON.stringify(schema)}, kind: ${kind}), expected one of these: ${JSON.stringify(ALL_VALID_PRIMS)}`)
            }
        }
        if (isKind(kind, KIND_SUB)) {
            this.rtValidateSubSchema(schema as unknown as SubSchema)
        }
        if (isKind(kind, KIND_ROOT)) {
            this.rtValidateRootSchema(schema as RootSchema)
        }

    }
    private rtValidateMixArraySchema(schema: (ValidPrimitives |  SubSchema)[]) {
        for (const val of schema) {
            if (!isValidPrimitive(val, true)) {
                this.rtValidateSubSchema(val as SubSchema)
            }
        }
    }
    private rtValidateSubSchema(schema: SubSchema) {
        for (const [k, v] of Object.entries(schema)) {
            const kind = this.getKind(v, true)
            this.rtValidateSchema(kind, v, k)
        }
    }
    private rtValidateRootSchema(schema: RootSchema) {
            console.log("rtValidateRootSchema", schema)
            for (const [k, v] of Object.entries(schema)) {
                console.log(k, ":", v)
                if (v.pointer) {
                    if (!isValidPrimitive(v.type)) {
                        throw new Error(`[RT-SCHEMA-CHECK] Invalid pointer RootSchema (key: ${k}) because it's not a primitive, or an optional primitive type (received: ${JSON.stringify(schema, null, 2)}, expected: ${JSON.stringify({type: `ONE OF: ${JSON.stringify(ALL_VALID_NOOPT_PRIMS)}`, pointer: true})}`)
                    }
                } else {
                    if (v.type && v.pointer !== undefined) {
                        const kind = this.getKind(v.type, true, k)
                        this.rtValidateSchema(kind, v.type, k)
                    } else {
                        const kind = this.getKind(v, true, k)
                        this.rtValidateSchema(kind, v, k)
                    }
                }
            }
    }

    public getKeyType(key: keyof T) {
        return this.schema[key]
    }

    public getPointerKeys(): string[] {
        const keys = collectKeysWhere(this.schema as Record<string, any>, (_, val) => val?.pointer === true)
        if (this.doStrictRtTypeChecks) return keys.filter(val => isValidPrimitive(val))
        return keys
    }

    private rtValidateObjectAgainst(against: SubSchema, obj: Record<string, any>): Result<null, string> {
        for (const [k, typ] of Object.entries(against)) {
            const toValidate = obj[k]
            if (isValidPrimitive(typ)) {
                if (isPrimitiveValue(toValidate)) {
                    const candidateType = resolvePrimitiveType(toValidate)
                    if (equals(candidateType, typ as ValidPrimitives)) {
                        continue
                    } else {
                        return Err(`[RT-QUERY-CHECK]: Invalid type in array DTO, expected: ${typ}, received: ${candidateType}`)
                    }
                } else {
                    return Err(`[RT-QUERY-CHECK]: Invalid type in array DTO, expected: ${typ}, received: ${JSON.stringify(toValidate)}`)
                }
            }
            if (Array.isArray(typ)) {
                if (Array.isArray(toValidate)) {
                    const res = this.rtValidateArrayAgainst(typ, toValidate)
                    if (res.isErr()) {
                        return res
                    }
                    continue
                } else {
                    return Err(`[RT-QUERY-CHECK]: Invalid type in array DTO, expected: ${JSON.stringify(typ)}, received: ${JSON.stringify(toValidate)}`)
                }
            }
            if (typeof typ === "object") {
                if (typeof toValidate === "object") {
                    const res = this.rtValidateObjectAgainst(<SubSchema>typ, toValidate)
                    if (res.isErr()) {
                        return res
                    }
                    continue
                } else {
                    return Err(`[RT-QUERY-CHECK]: Invalid type in array DTO, expected: ${JSON.stringify(typ)}, received: ${JSON.stringify(toValidate)}`)
                }
            }


        }
    }
    private rtValidateArrayAgainst(against: readonly ValidPrimitives[] | readonly SubSchema[], arr: readonly any[]): Result<null, string> {
        for (const [idx, typ] of against.entries()) {
            const toValidate = arr[idx]
            if (isValidPrimitive(typ)) {
                if (isPrimitiveValue(toValidate)) {
                    const candidateType = resolvePrimitiveType(toValidate)
                    if (equals(candidateType, typ as ValidPrimitives)) {
                        continue
                    } else {
                        return Err(`[RT-QUERY-CHECK]: Invalid type in array DTO, expected: ${typ}, received: ${candidateType}`)
                    }
                } else {
                    return Err(`[RT-QUERY-CHECK]: Invalid type in array DTO, expected: ${typ}, received: ${JSON.stringify(toValidate)}`)
                }
            }
            if (Array.isArray(typ)) {
                if (Array.isArray(toValidate)) {
                    const res = this.rtValidateArrayAgainst(typ, toValidate)
                    if (res.isErr()) {
                        return res
                    }
                    continue
                } else {
                    return Err(`[RT-QUERY-CHECK]: Invalid type in array DTO, expected: ${JSON.stringify(typ)}, received: ${JSON.stringify(toValidate)}`)
                }
            }

            if (typeof typ === "object") {
                if (typeof toValidate === "object") {
                    const res = this.rtValidateObjectAgainst(typ, toValidate)
                    if (res.isErr()) {
                        return res
                    }
                    continue
                } else {
                    return Err(`[RT-QUERY-CHECK]: Invalid type in array DTO, expected: ${JSON.stringify(typ)}, received: ${JSON.stringify(toValidate)}`)
                }
            }
        }
    }

    public rtValidateWriteOperation(query: Partial<SimplifySchema<Const<T>>>): Result<null, string> {
            const queryIsArray = Array.isArray(query)
            const queryIsPrimitive = isPrimitiveValue(query)
            const queryIsObject = typeof query === "object"
            if (queryIsPrimitive) {
                if (this.kind === KIND_PRIMITIVE) {
                    return Ok(null)
                } else {
                    return Err(`[RT-QUERY-CHECK]: Invalid write operation DTO. Expected kind: ${this.kind}, but received: ${JSON.stringify(query)}`)
                }
            }
            if (queryIsArray) {
                if (isKind(this.kind, [KIND_MULTI_ARRAY, KIND_SUB_ARRAY, KIND_PRIMITIVE_ARRAY])) {
                    return this.rtValidateArrayAgainst(<readonly ValidPrimitives[] | readonly SubSchema[]>this.schema, query)
                } else {
                    return Err(`[RT-QUERY-CHECK]: Invalid write operation DTO. Expected kind: ${this.kind}, but received: ${JSON.stringify(query)}`)
                }
            }
            if (queryIsObject) {
                if (isKind(this.kind, [KIND_SUB, KIND_ROOT])) {
                    return this.rtValidateObjectAgainst(<SubSchema>this.createFlatSchema(), query)
                } else {
                    return Err(`[RT-QUERY-CHECK]: Invalid write operation DTO. Expected kind: ${this.kind}, but received: ${JSON.stringify(query)}`)
                }
            }
    }
    private createFlatSchema() {
        if (this.kind === KIND_ROOT) {
            return (<RootSchema>this.schema).type
        }
        return this.schema
    }

}

//small function to check a schema (mainly useful for exports)
export function validateSchema(schema: ValidSchemas): Result<true, string> {
    try {
        new SchemaManager(schema, true)
        return Ok(true)
    }
    catch (err) {
        return Err(err.toString())
    }
}