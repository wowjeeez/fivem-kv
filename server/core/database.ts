import {Err, Ok, Result} from "../utils/result";
import {QueryError} from "../errors/query-error";
import {Serializer} from "./serializer";


interface TableQuery {
    queryString: string
    fieldName: string
    isMasterKey: boolean
    limit: number
    page: number
}



export class Table extends Serializer {
    constructor(private readonly name: string) {
        super()
    }
    public async getExact<T>(key: string): Promise<Result<T, QueryError>> {
        const value = GetResourceKvpString(key)
        if (value) {
          return Ok<T, QueryError>(this.deserialize<T>(value) as T)
        }
        return Err(new QueryError(this.name, key, "No value found"))
    }

    public async query<T>(query: TableQuery): Promise<T[]> {
            const key = this.createKeyPrefix(query.queryString, query.isMasterKey)
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
                        let val: any = GetResourceKvpString(key)
                        if (this.isFieldKey(val)) {
                            val = GetResourceKvpString(val)
                            if (val) {
                                val = this.deserialize<T>(val)
                            }
                        } else { //actual value resolving
                            val = this.deserialize<T>(val)
                        }
                        results.push(val as T)
                    } else {
                        break
                    }
                }
            }
            return results


    }
    protected createKeyPrefix(field: string, isMaster: boolean) {
        if (isMaster) {
            return `__TBL:${this.name}-${field}`
        }
        return `__TBL:${this.name}-__FLD:${field}-`
    }
    protected isFieldKey(candidate: string) {
        return candidate.includes("__FLD")
    }
}

