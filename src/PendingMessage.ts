import { randomUUID } from 'node:crypto';

export class PendingMessage {
    constructor(
        public readonly payload: Buffer,
        public readonly headers: Record<string, unknown> = {},
        public readonly at: Date = new Date(),
        public readonly id: string = randomUUID()
    ) {}
}
