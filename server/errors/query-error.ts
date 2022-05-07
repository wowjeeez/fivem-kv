export class QueryError extends Error {
    constructor(private readonly table: string, private readonly key: string, private readonly reason: string, private readonly props: string[] = []) {
        super()
    }

    public toString() {
        return `Error querying data under key: ${this.key} from ${this.table} due to: ${this.reason}.\n${this.props.join("\n")}` //TODO! formatting
    }
}