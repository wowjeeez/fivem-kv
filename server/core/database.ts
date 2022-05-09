import {Err, Ok, Result} from "../utils/result";
import {QueryError} from "../errors/query-error";
import {Serializer} from "./serializer";
import {PTR_KEY, TBL_KEY} from "./constants";
import {DeserializeError} from "../errors/deser-error";


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
type ValidPrimitives = "str" | "int" | "bool" | "float"

type RootSchema = {
   readonly [key: string]: {type: ValidPrimitives | SubSchema | readonly ValidPrimitives[] | readonly SubSchema[], pointer: boolean}
}

type SubSchema = {
    readonly [key: string]: ValidPrimitives | SubSchema | readonly ValidPrimitives[] | readonly SubSchema[]
}

export type ValidSchemas = RootSchema | readonly ValidPrimitives[] |  readonly SubSchema[] | ValidPrimitives
//just a type coercer
export function schema<T extends ValidSchemas>(obj: T): ToReadonly<T>["inner"] {
    return obj
}

type QueryableKeys<T extends RootSchema> = {
    [K in keyof T]: T[K]["pointer"] extends true ? K : never
}[keyof T]

type Queries<T, CanQuerySub extends boolean> = MasterTableQuery<T> | CanQuerySub extends true ? TableQuery<T> : MasterTableQuery<T>

type ConvertType<T extends ValidPrimitives | object> = T extends "str" ? string : T extends "int" ? number : T extends "float" ? number : T extends "bool" ? boolean : T


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
        T extends readonly SubSchema[] ? SimplifySub<T[number]>[] : T extends ValidPrimitives ? ConvertType<T> : never

const testSchema = schema({testKey: {type: {playerName: "str", playerRank: "int", medals: [{type: "str", grantedOn: "str"}]}, pointer: true}})
//convert a value to a const (only compile time, to get better type check results)
interface ToReadonly<T> {
    readonly inner: T
}

export class Table<T extends ValidSchemas, CanQuerySubKeys extends T extends RootSchema ? true : false, Queryable extends QueryableKeys<T extends RootSchema ? T : never>> extends Serializer {
    constructor(private readonly name: string, private readonly schema: T) {
        super()
    }
    public async getExact<T>(key: string): Promise<QueryResult<T>> {
        const value = GetResourceKvpString(key)
        if (value) {
          return this.deserialize<T>(value)
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
                val = this.deserialize(val)
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

    public writeToKey(key: string, queryObj: SimplifySchema<ToReadonly<T>["inner"]>) {

    }
}
//test

const table = new Table("hello", testSchema)
table.writeToKey("hello", {testKey: {playerName: "", playerRank: 10, medals: [{type: "str", grantedOn: "str"}]}})