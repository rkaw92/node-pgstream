# pgstream

This **Node.js** library lets you use **PostgreSQL** for **event streaming**. It gives you:
* Persistent, queue-like (FIFO) data structures
* Non-destructive consumption (via consumer offsets)
* Full durability of messages and consumer offsets
* Transactional publish and consume via knex.js
* Low to moderate throughput (expect ~100 msg/s)

## Install

```sh
npm install pgstream knex
```

## Produce messages

```typescript
import knex from 'knex';
import { Admin, ProducerBuilder, PendingMessage } from 'pgstream';
const db = knex({
    client: 'pg',
    connection: process.env.POSTGRES_URL
});
// Prepare the necessary tables - this is safe to run multiple times:
const admin = new Admin(db);
await admin.install();
await admin.createStream('myevents');
// Create our producer, now that it has a place to write to:
const producer = new ProducerBuilder()
    .withKnex(db)
    .stream('myevents')
    .build();
await producer.produce(new PendingMessage(
    Buffer.from('hello, world!'),
    { fooheader: 'bar' }
));
```

### How do I produce a message in an existing database transaction?

```typescript
// db is a Knex instance
db.transaction(async (trx) => {
    const producer = new ProducerBuilder()
        .inTransaction(trx)
        .stream('myevents')
        .build();
    // and now produce as usual
});
```

### How do I produce a message into a non-knex transaction?

(TODO: Describe how to implement a custom TransactionProvider.)

## Consume messages

```typescript
import knex from 'knex';
import { Admin, ProducerBuilder, PendingMessage } from 'pgstream';
const db = knex({
    client: 'pg',
    connection: process.env.POSTGRES_URL
});
// We also ensure the stream exists in the consumer code - this way, it doesn't matter
//  which part you run first, producer or consumer.
const admin = new Admin(db);
await admin.install();
await admin.createStream('myevents');
// Let's receive messages from the producer!
const consumer = new ConsumerBuilder()
        .withKnex(db)
        .stream('myevents')
        .name('myconsumer1')
        .handler(async (msg, trx) => {
            // If you want, you can use trx, which is a Knex transaction that's started
            //  with isolation level set to DB session default.
            console.log(msg);
            // When the Promise returned by the handler fulfills, the consumer's
            //  offset is advanced by 1, so it will subsequently get the next message.
            // On the other hand, if it rejects, the entire transaction is rolled
            //  back and the handler is re-run with the same message again.
            // This is effectively one-time message processing.
        }).build();
        await consumer.run();
        // Some time later, perhaps:
        // consumer.destroy();
```

Offsets are tracked per consumer **name**. If you run several consumers with the same name at the same time, only one will be active due to locking. This ensures true FIFO semantics, but disallows parallelism - **consumer groups**, as found in other, more advanced platforms, are **not supported**.

In messaging terms, this means that every consumer is exclusive and has a prefetch count of 1. Therefore, the only benefit from running multiple instances of a given consumer is failover.

### How do I get a SERIALIZABLE transaction in my handler?

Configure the Knex pool to change the session characteristics and set a new default isolation level:

* https://knexjs.org/guide/#aftercreate
* https://www.postgresql.org/docs/current/sql-set-transaction.html

(TODO: Provide a complete example.)

## Use cases

Use this library when your application already uses PostgreSQL and you want to avoid introducing another technology such as Apache Kafka. It was created with on-site and home usage in mind, especially for running on edge devices like Raspberry Pi where memory is limited and adding more memory-hungry software (JVM), may be a problem.

Do not use this library for high-throughput systems. The following solutions may be a better fit if you need serious streaming capabilities:
* Apache Kafka
* Apache Pulsar
* RabbitMQ Streams
* NATS Streaming
