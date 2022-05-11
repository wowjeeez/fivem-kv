import {Err, Ok, Result} from "../utils/result";
import {QueryError} from "../errors/query-error";
import {Serializer} from "./serializer";
import {PTR_KEY, TBL_KEY} from "./constants";
import {DeserializeError} from "../errors/deser-error";
import {SchemaManager} from "./schema";


interface TableQuery<T> {
    readonly queryString: string
    readonly isMasterKey: false
    readonly fieldName: T
    readonly limit: number
    readonly page: number
}
interface MasterTableQuery<T> {
    readonly queryString: string
    readonly isMasterKey: true
    readonly fieldName: null
    readonly limit: number
    readonly page: number
}

//empty error interface to serve as a marker for the QueryResult type
interface VoidErrMarker {
___NOOP: null
}

type QueryResult<T, ToOmit = VoidErrMarker> = Result<boolean | number | string | T, Exclude<QueryError | DeserializeError, ToOmit>>
export type ValidPrimitives =  RequiredPrimitives | OptionalPrimitives
type RequiredPrimitives = "str" | "int" | "bool" | "float" | "any"
type OptionalPrimitives = "str?" | "int?" | "float?" | "bool?" | "any?"


type OptPrimitiveMap = {
    "str?": string | undefined
    "int?": number | undefined
    "float?": number | undefined
    "bool?": boolean | undefined
    "any?": any | undefined
}


export type RootSchema = NoPtrRootSchema | PtrRootSchema

type NoPtrRootSchema = {
   readonly [key: string]: {type: ValidPrimitives | SubSchema | readonly ValidPrimitives[] | readonly SubSchema[], pointer: false}
}

type PtrRootSchema = {
    readonly [key: string]: {type: RequiredPrimitives, pointer: true}
}

export type SubSchema = {
    readonly [key: string]: readonly ValidPrimitives[] |  readonly SubSchema[] | ValidPrimitives | SubSchema
}

export type ValidSchemas = RootSchema | readonly ValidPrimitives[] |  readonly SubSchema[] | ValidPrimitives | SubSchema
//just a type coercer
export function schema<T extends ValidSchemas>(obj: T): Const<T> {
    return obj
}

type QueryableKeys<T extends RootSchema> = {
    [K in keyof T]: T[K]["pointer"] extends true ? K : never
}[keyof T]

type Queries<T, CanQuerySub extends boolean> = MasterTableQuery<T> | CanQuerySub extends true ? TableQuery<T> : MasterTableQuery<T>

//from schema to x conversion
type ConvertOptType<T extends keyof OptPrimitiveMap | object> = T extends keyof OptPrimitiveMap ? OptPrimitiveMap[T] : T

type ConvertType<T extends ValidPrimitives | object | keyof OptPrimitiveMap> = T extends "str" ? string :
    T extends "int" ? number :
        T extends "float" ? number :
            T extends "bool" ? boolean :
                T extends "any" ? any :
                    T extends keyof OptPrimitiveMap ? ConvertOptType<T> : T


type SimplifyRoot<T extends RootSchema> = {
    [K in keyof T]: T[K]["type"] extends ValidPrimitives ? ConvertType<T[K]["type"]> :
        T[K]["type"] extends SubSchema ? SimplifySub<T[K]["type"]> :
            T[K]["type"] extends readonly ValidPrimitives[] ? ConvertType<T[K]["type"][number]>[] :
                T[K]["type"] extends readonly SubSchema[] ? SimplifySub<T[K]["type"][number]> : never
}


type SimplifySub<T extends SubSchema | readonly SubSchema[] | readonly ValidPrimitives[] | ValidPrimitives> = {
    [K in keyof T]: T[K] extends ValidPrimitives ? ConvertType<T[K]> :
        T[K] extends SubSchema ? SimplifySub<T[K]> :
            T[K] extends readonly ValidPrimitives[] ? ConvertType<T[K][number]> :
                T[K] extends readonly SubSchema[] ? SimplifySub<T[K][number]>[] : never
}


type SimplifySchema<T extends ValidSchemas> = T extends RootSchema ? SimplifyRoot<T> :
    T extends readonly ValidPrimitives[] ? ConvertType<T[number]>[] :
        T extends SubSchema ? SimplifySub<SubSchema> :
            T extends readonly SubSchema[] ? SimplifySub<T[number]>[] : T extends ValidPrimitives ? ConvertType<T> : never


//convert a value to a const (only compile time, to get better type check results)
interface ToReadonly<T> {
    readonly inner: T
}

export type Const<T> = ToReadonly<T>["inner"]

export class Table<T extends ValidSchemas, CanQuerySubKeys extends T extends RootSchema ? true : false, Queryable extends QueryableKeys<T extends RootSchema ? T : never>> {
    private readonly serializer = new Serializer()
    private readonly schema: SchemaManager<T>
    constructor(private readonly name: string, schema: T, private readonly doRtTypeChecks = false) {
        this.schema = new SchemaManager<T>(schema, doRtTypeChecks)
    }

    public async getExact<T>(key: string): Promise<QueryResult<T>> {
        const value = GetResourceKvpString(key)
        if (value) {
          return this.serializer.deserialize<T>(value)
        }
        return Err(new QueryError(this.name, key, "No value found"))
    }

    public async getPointerExact<T>(key: string): Promise<QueryResult<T>> {
        const ptrKey = await this.getExact<string>(key)
        if (ptrKey.isOk() && typeof ptrKey.unwrap() === "string") {
            return this.getExact<T>(ptrKey.unwrap() as string)
        } else {
            return Err(new QueryError(this.name, key, `Invalid pointer key: ${key}`))
        }
    }

    private async doPaginatedQueryWithAction<T>(query: Queries<Queryable, CanQuerySubKeys>, actionRoutine: (key: string, i: number) => T | Promise<T>, push = true) {
        const key = this.createKeyPrefix(query.queryString, query.isMasterKey, query.fieldName as string)
        const handle = StartFindKvp(key)
        const results: T[] = []
        const paginate = query.limit !== -1
        const skipResults = paginate ? query.limit * query.page : 0
        let i = 0
        let stop = false;
        if (handle !== -1) {
            while (!stop) {
                i++;
                const key = FindKvp(handle)
                if (key) {
                    if (paginate) {
                        if (i < skipResults) {
                            continue
                        }
                    }
                    if (push) {
                        results.push(await actionRoutine(key, i))
                    } else {
                        await actionRoutine(key, i)
                    }
                } else {
                    break
                }
            }
        }
        return results
    }
    public async query<T>(query: Queries<Queryable, CanQuerySubKeys>): Promise<(QueryResult<T>)[]> {
        return this.doPaginatedQueryWithAction<QueryResult<T>>(query, async (key) => {
            let val: any = GetResourceKvpString(key)
            if (this.isPointerKey(val)) { //actual value resolving (even if the key is just a pointer)
                val = await this.getPointerExact<T>(val)
            } else {
                val = this.serializer.deserialize(val)
            }
            return val
        })
    }

    /**
     * Checks if a pointer and a value behind it exists
     * @param pointer
     */
    public async doesPointerExist(pointer: string) {
        const res = await this.getExact<string>(pointer)
        return res.isOk() && typeof res.unwrap() === "string" && (await this.getExact(res.unwrap())).isOk()
    }

    public async delete(query: Queries<Queryable, CanQuerySubKeys>, deletePointers = true): Promise<void> {
        await this.doPaginatedQueryWithAction<void>(query, DeleteResourceKvp, false)
    }

    public async count(query: Queries<Queryable, CanQuerySubKeys>): Promise<number> {
        let ctr = 0;
        await this.doPaginatedQueryWithAction(query, () => ctr++, false)
        return ctr
    }
    protected createKeyPrefix<M extends boolean>(searchQuery: string, isMaster: M, fieldName: M extends true ? never : string) {
        if (isMaster) {
            return `${TBL_KEY}:${this.name}-${searchQuery}`
        }
        return `${TBL_KEY}:${this.name}-${PTR_KEY}:${fieldName}-${searchQuery}`
    }
    protected isPointerKey(candidate: string) {
        return candidate.includes(PTR_KEY)
    }

    public writeToKey(key: string, queryObj: SimplifySchema<Const<T>>) {

    }
    public updateKey(key: string, queryObj: Partial<SimplifySchema<Const<T>>>) {

    }
}

