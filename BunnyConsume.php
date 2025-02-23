<?php

declare(strict_types=1);

namespace Telephantast\BunnyTransport;

use Bunny\Message;
use Telephantast\MessageBus\Async\Consumer;
use Telephantast\MessageBus\Async\ObjectDenormalizer;
use Telephantast\MessageBus\Async\TransportConsume;

use function dump;
use function React\Async\await;

/**
 * @api
 */
final class BunnyConsume implements TransportConsume
{
    private const DEFAULT_PREFETCH_COUNT = 1;

    private BunnyMessageDecoder $messageDecoder;

    /**
     * @var \WeakMap<Consumer, \Closure(): void>
     */
    private \WeakMap $consumerToCancel;

    public function __construct(
        private readonly BunnyConnectionPool $connectionPool,
        ObjectDenormalizer $objectDenormalizer,
        private readonly int $prefetchCount = self::DEFAULT_PREFETCH_COUNT,
    ) {
        $this->messageDecoder = new BunnyMessageDecoder($objectDenormalizer);
        /** @var \WeakMap<Consumer, \Closure(): void> */
        $this->consumerToCancel = new \WeakMap();
    }

    /**
     * @psalm-suppress MissingThrowsDocblock
     */
    public function runConsumer(Consumer $consumer): void
    {
        $channel = $this->connectionPool->get()->channel();
        $channel->qos(prefetchCount: $this->prefetchCount);
        $consumerTag = $channel->consume(
            callback: function (Message $message) use ($channel, $consumer): void
            {
                $consumer->handle($this->messageDecoder->decode($message));
                $channel->ack($message);
            },
            queue: $consumer->queue,
        )->consumerTag;

        $this->consumerToCancel[$consumer] = static function () use ($channel, $consumerTag): void {
            $channel->cancel($consumerTag);
            $channel->close();
        };
    }

    /**
     * @throws \Throwable
     */
    public function stopConsumer(Consumer $consumer): void
    {
        $cancel = $this->consumerToCancel[$consumer] ?? null;

        if ($cancel !== null) {
            $cancel();
            unset($this->consumerToCancel[$consumer]);
        }
    }
}
