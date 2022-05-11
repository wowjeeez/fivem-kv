import type {RootSchema, SubSchema, ValidPrimitives, ValidSchemas} from "./database";
import {
    KIND_MULTI_ARRAY,
    KIND_PRIMITIVE,
    KIND_PRIMITIVE_ARRAY,
    KIND_ROOT,
    KIND_SUB,
    KIND_SUB_ARRAY,
    type SchemaRtKinds
} from "./constants";

//runtime schema utilities

//cool functional programming functions
const ALL_VALID_PRIMS = ["str", "bool", "int", "float", "float?", "str?", "int?", "bool?"]
const ALL_VALID_NOOPT_PRIMS = ["str", "bool", "int", "float"]

const isPrimitiveValue = (val: any) => ["string", "boolean", "number"].includes(typeof val)
const IsValidPrimitiveOpt = (val: any) => ALL_VALID_PRIMS.includes(val)
const isValidPrimitive = (val: any, allowOpt = false) => allowOpt ? IsValidPrimitiveOpt(val) : ALL_VALID_NOOPT_PRIMS.includes(val)
const isKind = (val: any, kinds: SchemaRtKinds | SchemaRtKinds[]) => (Array.isArray(kinds) ? kinds : [kinds]).includes(val) //array conversion required due to mixed strings



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
    private getKind<B extends boolean>(schema: ValidSchemas | SubSchema, throwErr?: B, key?: string): B extends true ? SchemaRtKinds : SchemaRtKinds | boolean {
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

    private rtValidateSchema(kind: SchemaRtKinds = this.kind, schema: ValidSchemas | SubSchema = this.schema, key?: string) {
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

}