<?php

declare(strict_types=1);

namespace Telephantast\BunnyTransport;

use Bunny\Channel;
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use Telephantast\MessageBus\Async\Exchange;
use Telephantast\MessageBus\Async\ObjectNormalizer;
use Telephantast\MessageBus\Async\TransportPublish;

use function React\Async\await;
use function React\Promise\all;

/**
 * @api
 */
final class BunnyPublish implements TransportPublish
{
    private readonly BunnyMessageEncoder $messageEncoder;

    private ConfirmListener $confirmListener;

    public function __construct(
        private readonly BunnyConnectionPool $connectionPool,
        ObjectNormalizer $objectNormalizer,
    )
    {
        $this->messageEncoder = new BunnyMessageEncoder($objectNormalizer);
        $this->confirmListener = new ConfirmListener();
    }

    /**
     * @throws \JsonException
     * @throws \Throwable
     */
    public function publish(array $envelopes): void
    {
        $channel = $this->connectionPool->get()->channel();
        $confirmListener = $this->confirmListener;
        $channel->confirmSelect($confirmListener);
        $promises = [];

        foreach ($envelopes as $envelope)
        {
            $exchange = $envelope->getStamp(Exchange::class)?->exchange ?? throw new \LogicException('No exchange stamp');
            $deferred = new Deferred();
            $promises[] = $deferred->promise();
            $deliveryTag = $channel->publish(...$this->messageEncoder->encode($envelope), exchange: $exchange);

            if ($deliveryTag !== false)
            {
                $confirmListener->registerEnvelope($deliveryTag, $deferred);
            }
            else
            {
                $deferred->reject(new \RuntimeException('Failed to publish message'));
            }
        }

        await(all($promises));
    }

}