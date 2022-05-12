const equals = (v1: any, v2: any) => JSON.stringify(v1) === JSON.stringify(v2)

function lookup<T extends Record<string, any>>(obj: T, toLookup: any): string {
        for (const [k, v] of Object.entries(obj)) {
            if (equals(v, toLookup)) {
                return `${k}`
            }
            if (typeof v === "object") {
                return `${k}:${lookup(v, toLookup)}`
            }
        }
}

export function lookupLocationInObject(obj: Record<string, any>, val: any, sep = "=>") {
    return fmt(lookup(obj, val), sep)
}

//stolen from stackoverflow
function isNumeric(str: string) {
    return !isNaN(str as unknown as number) && // use type coercion to parse the _entirety_ of the string (`parseFloat` alone does not do this)...
        !isNaN(parseFloat(str)) // ...and ensure strings of whitespace fail
}

function fmt(path: string, joiner = ":") {
    const parts = path.split(":")
    const final: string[] = []
    for (const [k,v] of parts.entries()) {
        if (isNumeric(v)) {
            final.push(`[${v}]`)
        } else {
            final.push(v)
        }
    }
    return final.join(joiner)
}