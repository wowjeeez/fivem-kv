export class DeserializeError extends Error {
    constructor(private readonly val: any, private readonly deserErr: any) {
        super(val);
    }
    public toString() {
        return `Failed to deserialize value ${this.val} due to: ${this.deserErr}`
    }
}