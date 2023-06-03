import { CHANNEL_PREFIX } from "./constants";
import { AlreadyInTransaction, Knex, TransactionProvider } from "./integration";
import { PendingMessage } from "./PendingMessage";
import { validStreamName } from "./validation";

export class Producer {
    private streamTable: string;
    private notificationChannelName: string;

    public  constructor(
        private transactionProvider: TransactionProvider,
        private streamName: string,
        dbSchemaName: string
    ) {
        if (!validStreamName(streamName)) {
            throw new Error('invalid stream name for producer: ' + streamName);
        }
        this.streamTable = dbSchemaName ? `${dbSchemaName}.${streamName}` : streamName;
        this.notificationChannelName = `${CHANNEL_PREFIX}${this.streamName}`;
    }

    public async produce(msg: PendingMessage) {
        await this.transactionProvider.transaction(async (trx) => {
            // NOTE: I've tested advisory locking instead, and it does not help much (~20%).
            await trx.raw('LOCK TABLE ?? IN EXCLUSIVE MODE', [ this.streamTable ]);
            await trx.raw('INSERT INTO ?? (seq, id, payload, headers) SELECT COALESCE(MAX(seq), 0)+1, ?, ?, ?::jsonb FROM ??', [
                this.streamTable,
                msg.id,
                msg.payload,
                JSON.stringify(msg.headers),
                this.streamTable
            ]);
            await trx.raw('SELECT pg_notify(?, NULL)', [ this.notificationChannelName ]);
        });
    }

    public static withProvider(provider: TransactionProvider, streamName: string, dbSchemaName = '') {
        return new this(provider, streamName, dbSchemaName);
    }

    public static withKnex(db: Knex, streamName: string, dbSchemaName = '') {
        return this.withProvider(db, streamName, dbSchemaName);
    }

    public static inTransaction(transaction: Knex.Transaction, streamName: string, dbSchemaName = '') {
        const provider = new AlreadyInTransaction(transaction);
        return this.withProvider(provider, streamName, dbSchemaName);
    }
}

export class ProducerBuilder {
    // Mandatory fields - must set before .build():
    private _transactionProvider?: TransactionProvider;
    private _streamName?: string;
    // Optional fields that can be left as default:
    private _dbSchemaName = '';
    
    withKnex(db: Knex) {
        this._transactionProvider = db;
        return this;
    }

    inTransaction(existingTransaction: Knex.Transaction) {
        this._transactionProvider = new AlreadyInTransaction(existingTransaction);
        return this;
    }

    stream(streamName: string) {
        this._streamName = streamName;
        return this;
    }

    schemaName(dbSchemaName: string) {
        this._dbSchemaName = dbSchemaName;
        return this;
    }

    build() {
        if (!this._transactionProvider) {
            throw new Error('transactionProvider not set');
        }
        if (!this._streamName) {
            throw new Error('stream name not set');
        }
        return new Producer(
            this._transactionProvider,
            this._streamName,
            this._dbSchemaName
        )
    }
}
