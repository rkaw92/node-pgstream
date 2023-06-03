export class PersistedMessage {
    constructor(
        public readonly seq: BigInt,
        public readonly id: string,
        public readonly at: Date,
        public readonly headers: Record<string, unknown>,
        public readonly payload: Buffer
    ) {}
}
