import { knex } from 'knex';
import test from 'node:test';
import { Admin } from '../Admin';
import { ConsumerBuilder } from '../Consumer';
import { PendingMessage } from '../PendingMessage';
import { ProducerBuilder } from '../Producer';
import { delay } from '../utils';
import { randomUUID } from 'node:crypto';

test('producer and consumer', async () => {
    const db = knex({
        client: 'pg',
        connection: process.env.POSTGRES_URL,
        searchPath: [ 'test' ]
    });

    await db.raw('CREATE SCHEMA IF NOT EXISTS ??', [ 'test' ]);

    const admin = new Admin(db);
    await admin.install();
    await admin.createStream('events');

    const producer = new ProducerBuilder()
        .withKnex(db)
        .stream('events')
        .build();

    const expectedMessages = new Set();
    for (let i = 0; i < 100; i += 1) {
        const payload = `hello, ${randomUUID()}`;
        expectedMessages.add(payload);
        await producer.produce(new PendingMessage(Buffer.from(payload), { key1: 'value1' }));
    }

    const consumer = new ConsumerBuilder()
        .withKnex(db)
        .stream('events')
        .name('produce-consume-test')
        .releaseConnectionImmediately()
        .handler(async (msg) => {
            // Check if we can retrieve all expected texts:
            const payload = msg.payload.toString('utf-8');
            expectedMessages.delete(payload);
            if (expectedMessages.size === 0) {
                consumer.destroy();
            }
        }).build();
        await consumer.run();
    await delay(3000);
    if (expectedMessages.size > 0) {
        throw new Error('failed to retrieve all of the produced test messages');
    }
    db.destroy();
});
