// BSD 3-Clause License
//
// Copyright (c) 2018, IBM Corporation
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, this
//   list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// * Neither the name of the copyright holder nor the names of its
//   contributors may be used to endorse or promote products derived from
//   this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE

import { AmqpOptions, DEFAULT_AMQP_OPTIONS } from "./options";

import { PromiseMap, ResourcePool } from "../containers";
import { UnimplementedError } from "../errors";
import { ResultMessage } from "../messages";
import { GetOptions, ResultBackend } from "../result_backend";
import {
    createTimeoutPromise,
    isNullOrUndefined,
    promisifyEvent,
} from "../utility";

import * as AmqpLib from "amqplib";

const logger = {
    info: (m: any) => {
        console.log('[RpcBackend]', m);
    },
    error: (m: any) => {
        console.error('[RpcBackend]', m);
    }
};

/**
 * RabbitMQ result backend using RPC and one queue per client.
 */
export class RpcBackend implements ResultBackend {
    static BACKOFF_ADD = 2;
    static BACKOFF_MAX = 30;

    private channels: ResourcePool<AmqpLib.Channel>;
    private connection: Promise<AmqpLib.Connection>;
    private consumer: Promise<AmqpLib.Channel>;
    private consumerTag: Promise<string>;
    private readonly options: AmqpOptions;
    private promises: PromiseMap<string, Message>;
    private readonly routingKey: string;
    private onMessageCallback: ((message: Message) => void) | null = null;
    private backoff = 0;

    /**
     * Constructs an RpcBackend with the given routing key and options.
     * Queues up connection, creation, queue assertion, and consumer handling.
     *
     * @param routingKey The name of the queue to consume from.
     * @param options Connection options for the RabbitMQ connections. If
     *                `undefined`, will connect to localhost:6379.
     */
    public constructor(routingKey: string, options?: AmqpOptions) {
        const DEFAULT_TIMEOUT: number = 86400000; // 1 year in milliseconds

        this.options = (() => {
            if (isNullOrUndefined(options)) {
                return DEFAULT_AMQP_OPTIONS;
            }

            return options;
        })();

        this.promises = new PromiseMap<string, Message>(DEFAULT_TIMEOUT);
        this.routingKey = routingKey;

        const { connection, channels, consumer, consumerTag } = this.connect();
        this.connection = connection;
        this.channels = channels;
        this.consumer = consumer;
        this.consumerTag = consumerTag;
    }

    private reconnect() {
        if (this.backoff < RpcBackend.BACKOFF_MAX) {
            this.backoff += RpcBackend.BACKOFF_ADD;
        }
        logger.info(`Trying again in ${this.backoff} seconds...`);
        setTimeout(() => {
            const { connection, channels, consumer, consumerTag } = this.connect();
            this.connection = connection;
            this.channels = channels;
            this.consumer = consumer;
            this.consumerTag = consumerTag;
        }, this.backoff * 1000);
    }

    private connect() {
        const connection = Promise.resolve(AmqpLib.connect(this.options));
        connection.then(conn => {
            logger.info(`Connected to ${this.options.hostname}`);
            this.backoff = 0;
            conn.on('close', () => {
                logger.error('Connection dropped!');
                this.reconnect();
            });
            conn.on('error', (err) => {
                logger.error(`Connection failure! Waiting on restart. ${err}`);
            });
        }, err => {
            logger.error(`Connection failed: ${err}`);
            this.reconnect();
        });
        const channels = new ResourcePool(
            async () => {
                const conn = await connection;
                return conn.createChannel();
            },
            async channel => {
                await channel.close();
                return "closed";
            },
            2,
        );
        const consumer = channels.get();
        const consumerTag = consumer.then(consumer =>
            this.assertQueue(consumer).then(
                () => this.createConsumer(consumer),
                _ => ''
            ),
            _ => ''
        );
        return { connection, channels, consumer, consumerTag };
    }

    /**
     * @param message The message to queue.
     * @returns The response from RabbitMQ.
     */
    public async put<T>(message: ResultMessage<T>): Promise<string> {
        const toSend = Buffer.from(JSON.stringify(message), "utf8");
        const options = RpcBackend.createPublishOptions(message);

        return this.channels.use(async (channel) => {
            await this.assertQueue(channel);

            return this.sendToQueue({ channel, options, toSend });
        });
    }

    /**
     * Uses `Utility.createTimeoutPromise`.
     *
     * @param taskId The UUID of the task whose result is to be fetched.
     * @param timeout The time to wait, im milliseconds, before rejecting
     *                the promise. If `undefined`, will wait forever.
     * @returns The result as fetched from RabbitMQ.
     */
    public async get<T>({
        taskId,
        timeout
    }: GetOptions): Promise<ResultMessage<T>> {
        const raw = await this.promises.get(taskId);
        const result = RpcBackend.parseMessage<T>(raw);

        return createTimeoutPromise(result, timeout);
    }

    /**
     * @param taskId The UUID of the task whose result is to be deleted.
     * @returns "deleted" | "no result found".
     */
    public async delete(taskId: string): Promise<string> {
        if (this.promises.delete(taskId)) {
            return "deleted";
        }

        return "no result found";
    }

    /**
     * Gently closes all channels and the connection with RabbitMQ.
     * Alias for #end.
     *
     * @returns A `Promise` that resolves when the disconnection is complete.
     *
     * @see #end
     */
    public async disconnect(): Promise<void> {
        await this.end();
    }

    /**
     * Gently closes all channels and the connection with RabbitMQ.
     *
     * @returns A `Promise` that resolves when the disconnection is complete.
     */
    public async end(): Promise<void> {
        const consumer = await this.consumer;
        const consumerTag = await this.consumerTag;

        this.promises.rejectAll(new Error("disconnecting"));

        await consumer.cancel(consumerTag);
        this.channels.return(consumer);
        await this.channels.destroyAll();

        const connection = await this.connection;
        await connection.close();
    }

    /**
     * TODO: Actually return the connected RabbitMQ node's URI.
     *
     * @returns Nothing.
     *
     * @throws UnimplementedError Always.
     */
    public uri(): never {
        throw new UnimplementedError("Celery.Amqp.Backend.RpcBackend.uri");
    }

    public onMessage(callback: (message: Message) => void) {
        this.onMessageCallback = callback;
    }

    /**
     * Converts a message, assumed to be UTF-8 encoded, into an object
     * representation.
     *
     * @param message The raw message to parse.
     * @returns An object representation of `message`'s contents.
     *
     * @throws Error If the message does not contain valid UTF-8.
     * @throws SyntaxError If the message does not contain a valid
     *                     JSON-serialized object.
     */
    private static parseMessage<T>(message: Message): ResultMessage<T> {
        const content = message.content.toString("utf8");
        const parsed: ResultMessage<T> = JSON.parse(content);

        return parsed;
    }

    /**
     * Creates options for UTF-8 encoding, JSON serialization, non-persistent
     * transport, and 0 priority, with the UUID (`correlationId`) taken from
     * `message`.
     *
     * @param message The message to create publish options for.
     * @returns Options for publishing `message`.
     */
    private static createPublishOptions<T>(
        message: ResultMessage<T>
    ): AmqpLib.Options.Publish {
        return {
            contentEncoding: "utf-8",
            contentType: "application/json",
            correlationId: message.task_id,
            persistent: false,
            priority: 0,
        };
    }

    /**
     * Calls `AmqpLib.Channel#assertQueue`.
     *
     * @param channel The channel to use.
     * @returns The reply from RabbitMQ.
     */
    private async assertQueue(
        channel: AmqpLib.Channel
    ): Promise<AmqpLib.Replies.AssertQueue> {
        return channel.assertQueue(this.routingKey, {
            autoDelete: false,
            durable: false,
            expires: 86400000, // 1 day in ms
            messageTtl: 3600 * 1000,
        });
    }

    /**
     * Calls `AmqpLib.Channel#sendToQueue`. If the write buffer is full,
     * runs in a recursive loop triggered by the `"drain"` event being emitted.
     *
     * @param channel The channel to use.
     * @param options The options to publish with.
     * @param toSend The payload to write.
     */
    private async sendToQueue({ channel, options, toSend }: {
        channel: AmqpLib.Channel;
        options: AmqpLib.Options.Publish;
        toSend: Buffer;
    }): Promise<string> {
        const send = () => channel.sendToQueue(
            this.routingKey,
            toSend,
            options
        );

        while (!send()) {
            await promisifyEvent<void>(channel, "drain");
        }

        return "flushed to write buffer";
    }

    /**
     * Converts an `AmqpLib.Channel` into a consumer.
     *
     * @param consumer The `Channel` to use.
     * @returns A `Promise` that resolves to the consumer tag of the `Channel`.
     */
    private async createConsumer(consumer: AmqpLib.Channel): Promise<string> {
        const reply = await consumer.consume(
            this.routingKey,
            (message) => this.doOnMessage(consumer, message),
        );

        return reply.consumerTag;
    }

    /**
     * To run whenever a message is received. If the RabbitMQ server cancels
     * the consumer, all pending promises will be rejected.
     *
     * @param maybeMessage A message received from RabbitMQ. Will be null
     *                     if the consumer is cancelled.
     */
    private doOnMessage(consumer: AmqpLib.Channel, maybeMessage?: Message | null): void {
        if (isNullOrUndefined(maybeMessage)) {
            this.promises.rejectAll(new Error("RabbitMQ cancelled consumer"));
            return;
        }

        const message = maybeMessage;
        const id = message.properties.correlationId;

        if(this.promises.has(id)) {
            consumer.ack(message as AmqpLib.Message);
        } else {
            consumer.nack(message as AmqpLib.Message);
        }

        if (this.onMessageCallback) {
            this.onMessageCallback(message);
        }

        this.promises.resolve(id, message);
    }
}

/**
 * Layout of a RabbitMQ message.
 */
interface Message {
    content: Buffer;
    fields: object;
    properties: {
        contentEncoding: string;
        contentType: string;
        correlationId: string;
        headers: object;
        priority: number;
    };
}
