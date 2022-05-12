import {Err, Ok, Result} from "../utils/result";
import {QueryError} from "../errors/query-error";
import {Serializer} from "./serializer";
import {PTR_KEY, TBL_KEY} from "./constants";
import {DeserializeError} from "../errors/deser-error";
import {SchemaManager} from "./schema";


type TableQuery<T> = {
    readonly queryString: string
    readonly fieldName: T
    readonly limit: number
    readonly page: number
}

type MasterTableQuery = {
    readonly queryString: string
    readonly limit: number
    readonly page: number
}

type Query  = {
    readonly isMasterKey: boolean;
    readonly queryString: string
    readonly fieldName: string
    readonly limit: number
    readonly page: number
}


//empty error interface to serve as a marker for the QueryResult type, and to server as other dead-end types
interface VoidErrMarker {
___NOOP: null
}

type QueryResult<T, ToOmit = VoidErrMarker> = Result<T, Exclude<QueryError | DeserializeError, ToOmit>>
export type ValidPrimitives =  RequiredPrimitives | OptionalPrimitives
export type RequiredPrimitives = "str" | "int" | "bool" | "float" | "any"
type OptionalPrimitives = "str?" | "int?" | "float?" | "bool?" | "any?"


type OptPrimitiveMap = {
    "str?": string | undefined
    "int?": number | undefined
    "float?": number | undefined
    "bool?": boolean | undefined
    "any?": any | undefined
}


export type RootSchema = {
    readonly [key: string]: Const<NoPtrRootSchema> | Const<PtrRootSchema>
}

type NoPtrRootSchema = {readonly type: ValidPrimitives | SubSchema | readonly ValidPrimitives[] | readonly SubSchema[], readonly pointer: false}

type PtrRootSchema = {readonly type: RequiredPrimitives, readonly pointer: true}



export type SubSchema = {
    readonly [key: string]: readonly ValidPrimitives[] |  readonly SubSchema[] | ValidPrimitives | SubSchema
}

export type ValidSchemas = RootSchema | readonly ValidPrimitives[] |  readonly SubSchema[] | ValidPrimitives | SubSchema
//just a type coercer
export function schema<T extends ValidSchemas>(obj: T): Const<T> {
    return obj
}


export type QueryableKeys<T extends Record<string, NoPtrRootSchema | PtrRootSchema>> = Exclude<{
    readonly [K in keyof T]: T[K]["pointer"] extends true ? K : VoidErrMarker
}[keyof T], VoidErrMarker>


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
            T[K] extends readonly ValidPrimitives[] ? ConvertType<T[K][number]>[] :
                T[K] extends readonly SubSchema[] ? SimplifySub<T[K][number]>[] : never
}


export type SimplifySchema<T extends ValidSchemas> = T extends RootSchema ? SimplifyRoot<T> :
    T extends readonly ValidPrimitives[] ? ConvertType<T[number]>[] :
        T extends SubSchema ? SimplifySub<SubSchema> :
            T extends readonly SubSchema[] ? SimplifySub<T[number]>[] : T extends ValidPrimitives ? ConvertType<T> : never


//convert a value to a const (only compile time, to get better type check results)
interface ToReadonly<T> {
    readonly inner: T
}

export type Const<T> = ToReadonly<T>["inner"]

type PermitIf<PermitThis, IfThis, IsThis, IfNotThenThis> = IfThis extends IsThis ? PermitThis : IfNotThenThis

export class Table<
    T extends ValidSchemas,
    CanQuerySubKeys extends T extends RootSchema ? true : false,
    Queryable extends CanQuerySubKeys extends true ? T extends RootSchema ? QueryableKeys<Const<T>> : VoidErrMarker : VoidErrMarker,
    IsMasterFlag extends T extends RootSchema ? QueryableKeys<Const<T>> extends never ? true : boolean : true
    > {
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

    private async doPaginatedQueryWithAction<T>(query: Query, actionRoutine: (key: string, i: number) => T | Promise<T>, push = true) {
        const key = this.createKeyPrefix(query.queryString, query.isMasterKey, query.fieldName as unknown as string)
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
    private createQueryObject(query: MasterTableQuery | TableQuery<Queryable>, isMaster: boolean): Query {
       return {
           isMasterKey: isMaster,
           fieldName: (<any>query).fieldName || "",
           queryString: query.queryString,
           limit: query.limit,
           page: query.page
       }
    }
    public async query<M extends IsMasterFlag>(isMaster: M, query: M extends true ? MasterTableQuery : TableQuery<Queryable>): Promise<(QueryResult<SimplifySchema<Const<T>>>)[]> {
        return this.doPaginatedQueryWithAction<QueryResult<SimplifySchema<Const<T>>>>(this.createQueryObject(query, isMaster), async (key) => {
            let val: any = GetResourceKvpString(key)
            if (this.isPointerKey(val)) { //actual value resolving (even if the key is just a pointer)
                val = await this.getPointerExact(val)
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

    public async delete<M extends IsMasterFlag>(isMaster: M, query: M extends true ? MasterTableQuery : TableQuery<Queryable>, deletePointers = true): Promise<void> {
        await this.doPaginatedQueryWithAction<void>(this.createQueryObject(query, isMaster), DeleteResourceKvp, false)
    }

    public async count<M extends IsMasterFlag>(isMaster: M, query: M extends true ? MasterTableQuery : TableQuery<Queryable>): Promise<number> {
        let ctr = 0;
        await this.doPaginatedQueryWithAction(this.createQueryObject(query, isMaster), () => ctr++, false)
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
        if (this.doRtTypeChecks) {
            console.log("Validating write query")
            this.schema.rtValidateWriteOperation(queryObj).unwrap()
            console.log("Validated!")
        }
    }
    public updateKey(key: string, queryObj: Partial<SimplifySchema<Const<T>>>) {

    }
}

