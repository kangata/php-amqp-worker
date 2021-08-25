<?php

namespace QuetzalStudio\PhpAmqpWorker;

use App\Worker\Contracts\WithLogger;
use Carbon\Carbon;
use Exception;
use InvalidArgumentException;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use QuetzalStudio\PhpAmqpWorker\Log\Logger;

abstract class Worker
{
    /**
     * The base path for the worker.
     *
     * @var string
     */
    protected $basePath;

    /**
     * Config
     *
     * @var array
     */
    protected $config;

    /**
     * Connection
     *
     * @var \PhpAmqpLib\Connection\AMQPLazyConnection
     */
    protected $connection;

    /**
     * Channel
     *
     * @var \PhpAmqpLib\Channel\AMQPChannel
     */
    protected $channel;

    /**
     * Connection options
     *
     * @var array
     */
    protected $connectionOptions = [
        'heartbeat' => 0,
    ];

    /**
     * Worker logger
     *
     * @var \QuetzalStudio\PhpAmqpWorker\Log\Logger
     */
    protected $logger;

    /**
     * Worker start time
     *
     * @var \Carbon\Carbon
     */
    protected $startTime;

    /**
     * Incomming message time
     *
     * @var \Carbon\Carbon
     */
    protected $incommingMessageTime;

    /**
     * The worker instance
     *
     * @var \QuetzalStudio\PhpAmqpWorker\Worker
     */
    protected static $instance;

    public function __construct($basePath = null)
    {
        $this->registerShutdownEvent();

        $this->basePath = $basePath;

        $this->registerConfig();

        self::$instance = $this;
    }

    /**
     * Get worker instance
     *
     * @return mixed
     */
    public static function getInstance()
    {
        return self::$instance;
    }

    /**
     * Get application base path
     *
     * @param string $path
     * @return string
     */
    public function basePath($path = null): string
    {
        return $this->basePath.($path != null ? DIRECTORY_SEPARATOR.$path : $path);
    }

    /**
     * Register worker & logger config
     *
     * @return void
     */
    public function registerConfig()
    {
        if (class_exists(\Illuminate\Foundation\Application::class)) {
            $this->config = config("workers.{$this->name()}");

            $this->logger = new Logger(config('logging'));
        } else {
            $this->config = require "{$this->basePath}/config/worker.php";

            $loggingConfig = require "{$this->basePath}/config/logging.php";

            $this->logger = new Logger($loggingConfig);
        }
    }

    /**
     * Get app environment
     *
     * @return string
     */
    public function env()
    {
        return strtolower(env('APP_ENV', 'local'));
    }

    /**
     * Get config
     *
     * @param string|null $key
     * @return mixed
     */
    public function config($key = null)
    {
        if ($key != null && isset($this->config[$key])) {
            return $this->config[$key];
        }

        return $this->config;
    }

    /**
     * Get logger
     *
     * @return \Psr\Log\LoggerInterface
     */
    public function logger(): LoggerInterface
    {
        return $this->logger;
    }

    /**
     * Logger channel
     *
     * @return string
     */
    public function logChannel(): string
    {
        return 'stack';
    }

    /**
     * Get log instance
     *
     * @return \Monolog\Logger
     */
    public function log(): \Monolog\Logger
    {
        return $this->logger->channel($this->logChannel());
    }

    /**
     * Get worker name
     * This use for get config (config/workers.php)
     *
     * @return string
     */
    abstract public function name(): string;

    /**
     * Get alias name
     *
     * @return string
     */
    public function alias()
    {
        return $this->name();
    }

    /**
     * Get connection name
     *
     * @return string
     */
    public function connectionName()
    {
        return "{$this->alias()} # {$this->env()}";
    }

    /**
     * Get host
     *
     * @return string
     */
    public function host()
    {
        return $this->config('host');
    }

    /**
     * Get port
     *
     * @return int
     */
    public function port()
    {
        return $this->config('port');
    }

    /**
     * Get vhost
     *
     * @return string
     */
    public function vhost()
    {
        return $this->config('vhost');
    }

    /**
     * Get exchange
     *
     * @return string
     */
    public function exchange()
    {
        return $this->config('exchange');
    }

    /**
     * Get queue
     *
     * @return string
     */
    public function queue()
    {
        return $this->config('queue');
    }

    /**
     * Get routing key
     *
     * @return string
     */
    public function routingKey()
    {
        return $this->config('routing_key');
    }

    /**
     * Get plain url
     *
     * @return string
     */
    public function url()
    {
        $url = "{$this->host()}:{$this->port()}/{$this->vhost()}";

        return preg_replace("/\/\//", "/", $url);
    }

    /**
     * Get connection
     *
     * @return \PhpAmqpLib\Connection\AMQPLazyConnection
     */
    public function connection()
    {
        return $this->connection;
    }

    /**
     * Get channel
     *
     * @return \PhpAmqpLib\Channel\AMQPChannel
     */
    public function channel()
    {
        return $this->channel;
    }

    /**
     * Create connection
     *
     * @return \QuetzalStudio\PhpAmqpWorker\Worker
     * @throws \Exception
     */
    public function connect()
    {
        try {
            $hosts = [
                array_intersect_key($this->config(), array_flip([
                    'host', 'port', 'user', 'password', 'vhost',
                ])),
            ];

            AMQPLazyConnection::$LIBRARY_PROPERTIES['connection_name'] = ['S', $this->connectionName()];

            $this->connection = AMQPLazyConnection::create_connection($hosts, $this->connectionOptions);
        } catch (Exception $e) {
            throw $e;
        }

        try {
            $this->channel = $this->connection->channel();

            $this->log()->info("`{$this->alias()}` connected to {$this->url()}");
            $this->log()->info("`{$this->alias()}` connected to channel");
        } catch (Exception $e) {
            throw $e;
        }

        return $this;
    }

    /**
     * Declade exchage
     *
     * @return \QuetzalStudio\PhpAmqpWorker\Worker
     */
    public function declareExchange()
    {
        $this->channel()->exchange_declare(
            $this->exchange(),
            AMQPExchangeType::DIRECT,
            $passive = false,
            $durable = true,
            $autoDelete = false
        );

        return $this;
    }

    /**
     * Declare queue
     *
     * @return \QuetzalStudio\PhpAmqpWorker\Worker
     */
    public function declareQueue()
    {
        $this->channel()->queue_declare(
            $this->queue(),
            $passive = false,
            $durable = true,
            $exclusive = false,
            $autoDelete = false,
            $nowait = false,
            $arguments = new AMQPTable([])
        );

        return $this;
    }

    /**
     * Bind queue
     *
     * @return \QuetzalStudio\PhpAmqpWorker\Worker
     */
    public function bindQueue()
    {
        $this->channel()->queue_bind(
            $this->queue(),
            $this->exchange(),
            $this->routingKey()
        );

        $this->log()->info("`{$this->alias()}` has binded to {$this->queue()} queue");
        $this->log()->info("{$this->exchange()} --> {$this->routingKey()} --> {$this->queue()}");

        return $this;
    }

    /**
     * Consume message
     *
     * @return void
     */
    public function consume(): void
    {
        $this->channel()->basic_consume(
            $this->queue(),
            $consumerTag = '',
            $noLocal = false,
            $noAck = $this->config('no_ack'),
            $exclusive = false,
            $nowait = false,
            $callback = function ($message) {
                try {
                    $this->consumeMessage($message);
                } catch (Exception $e) {
                    $this->consumeMessageException($message, $e);
                }
            }
        );

        while ($this->channel()->is_consuming()) {
            $this->channel()->wait();
        }

        $this->channel()->close();
        $this->connection()->close();
    }

    /**
     * Get messagge headers
     *
     * @param \PhpAmqpLib\Message\AMQPMessage $message
     * @return array
     */
    public function getMessageHeaders(AMQPMessage $message): array
    {
        $properties = new AMQPTable($message->get_properties());
        $data = $properties->getNativeData();
        $headers = isset($data['application_headers'])
            ? $data['application_headers']
            : [];

        return $headers;
    }

    /**
     * Get messagge payload
     *
     * @param \PhpAmqpLib\Message\AMQPMessage $message
     * @return mixed
     */
    public function getMessagePayload(AMQPMessage $message)
    {
        return json_decode($message->body, true) ?? $message->body;
    }

    /**
     * Handle incoming message
     *
     * @param \PhpAmqpLib\Message\AMQPMessage $message
     * @return void
     */
    public function consumeMessage(AMQPMessage $message): void
    {
        $this->incommingMessageTime = Carbon::now();

        $this->log()->info('Received message', [
            'payload' => $this->getMessagePayload($message),
            'headers' => $this->getMessageHeaders($message),
        ]);

        $this->handle($message);

        $this->log()->info("Processed in {$this->getProcessTime()}");
    }

    /**
     * Handle incoming message exception
     *
     * @param \PhpAmqpLib\Message\AMQPMessage $message
     * @param \Exception $e
     * @return void
     */
    public function consumeMessageException(AMQPMessage $message, Exception $e): void
    {
        $this->log()->error("{$e->getMessage()} {$e->getFile()}:{$e->getLine()}");

        $this->onFail($message, $e);

        $this->log()->info("{$this->getProcessTime()}");
    }

    /**
     * Process incoming message
     *
     * @param \PhpAmqpLib\Message\AMQPMessage $message
     * @return void
     */
    abstract public function handle(AMQPMessage $message): void;

    /**
     * Handle if process fail
     *
     * @param \PhpAmqpLib\Message\AMQPMessage $message
     * @return void
     */
    abstract public function onFail(AMQPMessage $message, Exception $e): void;

    /**
     * Get duration time
     *
     * @param Carbon $start
     * @param Carbon $end
     * @return string
     */
    public static function durationOf(Carbon $start, Carbon $end): string
    {
        $duration = $end->diffInMilliseconds($start);
        $timeUnit = $duration > 1 ? 'milliseconds' : 'millisecond';

        if ($duration > (1000 * 60 * 60)) {
            $duration = $end->diffInHours($start);
            $timeUnit = $duration > 1 ? 'hours' : 'hour';
        } elseif ($duration > (1000 * 60)) {
            $duration = $end->diffInMinutes($start);
            $timeUnit = $duration > 1 ? 'minutes' : 'minute';
        } elseif ($duration > 1000) {
            $duration = $end->diffInSeconds($start);
            $timeUnit = $duration > 1 ? 'seconds' : 'second';
        }

        return "{$duration} {$timeUnit}";
    }

    /**
     * Get process time
     *
     * @return string
     */
    public function getProcessTime(): string
    {
        $finishedTime = Carbon::now();

        return self::durationOf($this->incommingMessageTime, $finishedTime);
    }

    private function registerShutdownEvent()
    {
        register_shutdown_function(function () {
            $error = error_get_last();

            if ($error['type'] === E_ERROR) {
                $message = preg_replace('/(\.php:\d+)(?s).*/', "$1", $error['message']);

                $this->log()->error($message);
            }

            if ($this->startTime) {
                $duration = $this->durationOf(
                    Carbon::parse($this->startTime),
                    Carbon::now()
                );

                $message = '`'. $this->alias() .'` # '. $this->env() ." stoped. Uptime {$duration}";

                $this->log()->info($message);
            }
        });
    }

    /**
     * Start worker
     *
     * @return void
     */
    public function start(): void
    {
        $this->startTime = microtime(true);

        try {
            $this->log()->info("`{$this->alias()}` # {$this->env()} started");

            $this->connect()->declareExchange()->declareQueue()->bindQueue()->consume();
        } catch (Exception $e) {
            $this->log()->error($e);
        }
    }
}
