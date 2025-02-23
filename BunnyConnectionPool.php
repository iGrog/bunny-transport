<?php

declare(strict_types=1);

namespace Telephantast\BunnyTransport;

use Bunny\Client;

/**
 * @api
 */
final class BunnyConnectionPool
{
    public function __construct(
        private readonly string $host = 'localhost',
        private readonly int $port = 5672,
        private readonly string $user = 'guest',
        private readonly string $password = 'guest',
        private readonly string $vhost = '/',
        private readonly int $heartbeatSeconds = 60,
    ) {}

    /**
     * @psalm-suppress MissingThrowsDocblock
     */
    public function get(): \Bunny\Client
    {
        $client = new Client([
            'host' => $this->host,
            'port' => $this->port,
            'user' => $this->user,
            'password' => $this->password,
            'vhost' => $this->vhost,
            'heartbeat' => $this->heartbeatSeconds,
        ]);

        return $client;
    }

    /**
     * @psalm-suppress MissingThrowsDocblock
     */
    public function disconnect(): void
    {
    }

    public function __destruct()
    {
        $this->disconnect();
    }
}