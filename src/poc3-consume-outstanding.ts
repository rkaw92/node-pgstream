import knex from 'knex';
import { Consumer, ConsumerBuilder } from './Consumer';
import { delay } from './utils';

const db = knex({
    client: 'pg',
    connection: process.env.POSTGRES_URL
});

(async function() {
    const consumer = new ConsumerBuilder()
        .withKnex(db)
        .schemaName('streams')
        .stream('events')
        .name('poc3')
        .handler(async (msg) => {
            console.log(msg);
            // await delay(2000);
        }).build();
})();
