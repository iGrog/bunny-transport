<?php

declare(strict_types=1);

namespace Telephantast\BunnyTransport;

use Telephantast\MessageBus\Async\TransportSetup;
use function React\Async\await;

/**
 * @api
 */
final readonly class BunnySetup implements TransportSetup
{
    public function __construct(
        private BunnyConnectionPool $connectionPool,
    ) {}

    /**
     * @psalm-suppress MissingThrowsDocblock
     */
    public function setup(array $exchangeToQueues): void
    {
        $client = $this->connectionPool->get();
        $channel = $client->channel();

        foreach ($exchangeToQueues as $exchange => $queues)
        {
            $channel->exchangeDeclare($exchange, 'fanout', durable: true);

            foreach ($queues as $queue)
            {
                $channel->queueDeclare($queue, durable: true);
                $channel->queueBind($exchange, $queue);
            }
        }
        $channel->close();
    }
}