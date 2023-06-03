import { CONSUMER_TABLE_NAME, STREAM_TEMPLATE_NAME } from "./constants";
import { TransactionProvider } from "./integration";

export class Admin {
    constructor(private provider: TransactionProvider, private schema = '') {}

    async install() {
        await this.provider.transaction(async (trx) => {
            await trx.raw(`CREATE TABLE IF NOT EXISTS ?? (
                seq BIGINT NOT NULL PRIMARY KEY,
                id UUID NOT NULL,
                at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                headers JSONB NOT NULL DEFAULT '{}'::jsonb,
                payload BYTEA
            )`, [
                this.schema ? `${this.schema}.${STREAM_TEMPLATE_NAME}` : STREAM_TEMPLATE_NAME
            ]);
            await trx.raw(`CREATE TABLE IF NOT EXISTS ?? (
                stream TEXT NOT NULL,
                name TEXT NOT NULL,
                last BIGINT NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                PRIMARY KEY (stream, name)
            )`, [
                this.schema ? `${this.schema}.${CONSUMER_TABLE_NAME}` : CONSUMER_TABLE_NAME
            ]);
        });
    }

    async createStream(name: string) {
        await this.provider.transaction(async (trx) => {
            await trx.raw(`CREATE TABLE IF NOT EXISTS ?? (
                LIKE ?? INCLUDING ALL
            )`, [
                this.schema ? `${this.schema}.${name}` : name,
                this.schema ? `${this.schema}.${STREAM_TEMPLATE_NAME}` : STREAM_TEMPLATE_NAME
            ]);
        });
    }
}
