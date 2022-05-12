export class ValidationError extends Error {
    constructor(private readonly val: string) {
        super(val);
    }
    public toString() {
        return this.val
    }
}