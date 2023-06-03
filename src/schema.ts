export interface ConsumerRow {
    stream: string;
    name: string;
    // bigint in Postgres maps to string in memory:
    last: string;
    updated_at: Date;
}

export interface MessageRow {
    // bigint, too
    seq: string;
    id: string;
    at: Date;
    headers: {};
    payload: Buffer;
}
