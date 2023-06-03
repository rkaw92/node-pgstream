import { delay } from "./utils";
import { TransactionProvider, Knex, ConnectionProvider } from "./integration";
import { PersistedMessage } from "./PersistedMessage";
import { ConsumerRow, MessageRow } from "./schema";
import { CHANNEL_PREFIX, CONSUMER_TABLE_NAME } from "./constants";

export interface MessageHandler {
    (msg: PersistedMessage, transaction: Knex.Transaction): void | Promise<void>;
}



export class Consumer {
    private notificationChannel;
    private streamTable;
    private consumersTable;
    private initialized = false;
    private destroyed = false;
    private connecting = false;
    private processing = false;
    private pending = false;
    private backupTimer?: NodeJS.Timeout;

    constructor(
        private transactionProvider: TransactionProvider,
        private connectionProvider: ConnectionProvider,
        private handler: MessageHandler,
        private name: string,
        private streamName: string,
        dbSchemaName: string,
        private last: BigInt = 0n,
        private delayOnFailure: number = 5000,
        private backupPollingInterval: number = 120000,
        private releaseConnectionImmediately: boolean = false
    ) {
        this.notificationChannel = `${CHANNEL_PREFIX}${streamName}`;
        this.streamTable = dbSchemaName ? `${dbSchemaName}.${streamName}` : streamName;
        this.consumersTable = dbSchemaName ? `${dbSchemaName}.${CONSUMER_TABLE_NAME}` : CONSUMER_TABLE_NAME;
    }

    async run() {
        if (this.initialized) {
            return;
        }
        this.initialized = true;
        this.maintainListener();
        this.backupTimer = setInterval(() => this.wakeup(), this.backupPollingInterval);
    }

    destroy() {
        if (this.destroyed) {
            return;
        }
        this.destroyed = true;
        clearInterval(this.backupTimer);
    }

    private async maintainListener() {
        if (this.connecting || this.destroyed) {
            return;
        }
        this.connecting = true;
        try {
            const conn = await this.connectionProvider.acquireConnection();
            conn.on('notification', ({ channel }) => {
                if (channel === this.notificationChannel) {
                    this.wakeup();
                }
            });
            conn.on('end', async () => {
                await delay(this.delayOnFailure);
                this.maintainListener();
            });
            await conn.query(`LISTEN "${this.notificationChannel}"`);
            if (this.releaseConnectionImmediately) {
                this.connectionProvider.releaseConnection(conn);
            }
            // Immediately after registering the listener, wake up the consumer in case
            //  we missed some notifications while disconnected:
            this.wakeup();
            this.connecting = false;
        } catch (error) {
            // TODO: Report this error.
            this.connecting = false;
            await delay(this.delayOnFailure);
            this.maintainListener();
        }
    }

    private wakeup() {
        this.pending = true;
        if (!this.processing) {
            this.consumeOutstandingWithRetry();
        }
    }

    private async consumeOutstandingWithRetry() {
        this.processing = true;
        let success = false;
        while (!this.destroyed && (!success || this.pending)) {
            try {
                // Clear the pending flag - but it may be set asynchronously again!
                this.pending = false;
                await this.consumeOutstanding();
                success = true;
            } catch (err) {
                console.error(err);
                await delay(this.delayOnFailure);
                // TODO: Better error reporting
            }
        }
        this.processing = false;
    }

    private async consumeOutstanding() {
        let end = false;
        while (!end && !this.destroyed) {
            await this.transactionProvider.transaction(async (trx) => {
                // Check where we're at and lock our entry at the same time:
                const myConsumerSelector = { stream: this.streamName, name: this.name };
                const myConsumerState = await trx<ConsumerRow>(this.consumersTable)
                    .select([ 'last' ])
                    .where(myConsumerSelector)
                    .forUpdate()
                    .first();
                if (!myConsumerState) {
                    // Pre-create the state so that it can be locked, retry:
                    await trx<ConsumerRow>(this.consumersTable).insert({
                        stream: this.streamName,
                        name: this.name,
                        last: this.last.toString(10),
                        updated_at: new Date()
                    });
                    return;
                }
                // Grab a new message:
                const newMessageSeq = (BigInt(myConsumerState.last) + 1n).toString();
                const messageRow = await trx<MessageRow>(this.streamTable)
                    .select([ 'seq', 'id', 'at', 'headers', 'payload' ])
                    .where({
                        seq: newMessageSeq
                    }).first();
                if (!messageRow) {
                    end = true;
                    return;
                }
                const messageObject = new PersistedMessage(
                    BigInt(messageRow.seq),
                    messageRow.id,
                    messageRow.at,
                    messageRow.headers,
                    messageRow.payload
                );
                try {
                    await this.handler(messageObject, trx);
                    await trx<ConsumerRow>(this.consumersTable).update({
                        last: newMessageSeq,
                        updated_at: new Date()
                    }).where(myConsumerSelector);
                } catch (error) {
                    await delay(this.delayOnFailure);
                    return;
                }
            });
        }
    }
}

export class ConsumerBuilder {
    // Mandatory fields - must set before .build():
    private _transactionProvider?: TransactionProvider;
    private _connectionProvider?: ConnectionProvider;
    private _name?: string;
    private _streamName?: string;
    private _handler?: MessageHandler;
    // Optional fields that can be left as default:
    private _dbSchemaName = '';
    private _last: BigInt = 0n;
    private _delayOnFailure = 5000;
    private _backupPollingInterval = 120000;
    private _releaseConnectionImmediately = false;

    withKnex(db: Knex) {
        this._transactionProvider = db;
        this._connectionProvider = db.client;
        return this;
    }

    setTransactionProvider(provider: TransactionProvider) {
        this._transactionProvider = provider;
        return this;
    }

    setConnectionProvider(provider: ConnectionProvider) {
        this._connectionProvider = provider;
        return this;
    }

    name(name: string) {
        this._name = name;
        return this;
    }

    stream(streamName: string) {
        this._streamName = streamName;
        return this;
    }

    handler(handler: MessageHandler) {
        this._handler = handler;
        return this;
    }

    schemaName(dbSchemaName: string) {
        this._dbSchemaName = dbSchemaName;
        return this;
    }

    last(last: BigInt) {
        this._last = last;
        return this;
    }

    startAfter(last: BigInt) {
        return this.last(last);
    }

    setDelayOnFailure(delayOnFailure: number) {
        this._delayOnFailure = delayOnFailure;
        return this;
    }

    setBackupPollingInterval(backupPollingInterval: number) {
        this._backupPollingInterval = backupPollingInterval;
        return this;
    }

    releaseConnectionImmediately() {
        this._releaseConnectionImmediately = true;
        return this;
    }

    build() {
        if (!this._transactionProvider) {
            throw new Error('transactionProvider not set');
        }
        if (!this._connectionProvider) {
            throw new Error('connectionProvider not set');
        }
        if (!this._name) {
            throw new Error('consumer name not set');
        }
        if (!this._streamName) {
            throw new Error('stream name not set');
        }
        if (!this._handler) {
            throw new Error('message handler not set');
        }
        return new Consumer(
            this._transactionProvider,
            this._connectionProvider,
            this._handler,
            this._name,
            this._streamName,
            this._dbSchemaName,
            this._last,
            this._delayOnFailure,
            this._backupPollingInterval,
            this._releaseConnectionImmediately
        );
    }
}
