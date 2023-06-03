import { Knex } from 'knex';
import { PoolClient } from 'pg';

type RawConnectionForListening = Pick<PoolClient, "query" | "on">;

/**
 * A TransactionProvider is an object that allows executing a given block of code
 *  in a single SQL transaction. It must guarantee 3 things:
 * - The transaction stays active until the executor promise has resolved
 * - The transaction commits after the promise is fulfilled
 * - The transaction rolls back after the promise is rejected
 * 
 * It does not have to guarantee that the executor function will be the only thing
 *  running in this transaction. In fact, it may be beneficial to bundle together
 *  multiple executors in one transaction. Therefore, the guarantees that the
 *  provider gives are minimums only.
 */
export interface TransactionProvider {
    transaction<TReturn>(executor: (trx: Knex.Transaction) => Promise<TReturn>): Promise<TReturn>;
}

export class AlreadyInTransaction implements TransactionProvider {
    constructor(private existingTransaction: Knex.Transaction) {}
    transaction<TReturn>(executor: (trx: Knex.Transaction<any, any[]>) => Promise<TReturn>): Promise<TReturn> {
        return executor(this.existingTransaction);
    }
    
}

export interface ConnectionProvider {
    acquireConnection(): Promise<RawConnectionForListening>;
    releaseConnection(conn: RawConnectionForListening): unknown;
}

export { Knex };
