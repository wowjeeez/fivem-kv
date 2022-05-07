class IResult<T, E> {
    private constructor(private readonly okData: T, private readonly errData: E, private readonly success: boolean) {}
    public static createOk<T, Err = void>(val: T): Result<T, Err> {
            return new IResult<T, Err>(val, null, true)
    }
    public static createErr<T, Succ = void>(val: T): Result<Succ, T> {
        return new IResult<Succ, T>(null, val, false)
    }
    public unwrap(): T {
        if (this.success) {
            return this.okData
        }
        throw new Error(`Called .unwrap() on an Err result\n${this.errData}`)
    }
    public isErr() {
        return !this.success
    }
    public isOk() {
        return this.success
    }
    public unwrapOr(or: T) {
        return this.success ? this.okData : or
    }

    public unwrapErr() {
        if (this.success) {
            throw new Error(`Called .unwrapErr() on an Ok result\n${this.okData}`)
        } else {
            return this.errData
        }
    }

}

export function Ok<T, Err = void>(val: T): Result<T, Err> {
    return IResult.createOk(val)
}

export function Err<T, Succ = void>(val: T): Result<Succ, T> {
    return IResult.createErr<T, Succ>(val)
}

export function wrapIntoResult<T extends (...args: any) => any>(func: T): Result<ReturnType<T>, any> {
    try {
        const res = func()
        return Ok(res)
    } catch (err) {
        return Err<any, ReturnType<T>>(err)
    }
}

export type Result<T, E> = IResult<T, E>