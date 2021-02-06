<?php

namespace Zhineng\Tablestore;

use Aliyun\OTS\Consts\ColumnTypeConst;
use Aliyun\OTS\Consts\PrimaryKeyTypeConst;
use Aliyun\OTS\OTSClient;
use DateTimeInterface;
use Illuminate\Contracts\Cache\Store;
use Illuminate\Support\Carbon;
use Illuminate\Support\InteractsWithTime;

class TablestoreStore implements Store
{
    use InteractsWithTime;

    protected int $columnValueIndex = 1;

    protected int $columnTimestampIndex = 3;

    public function __construct(
        protected OTSClient $tablestore,
        protected string $table,
        protected string $keyAttribute = 'key',
        protected string $valueAttribute = 'value'
    ) {}

    public function get($key)
    {
        $response = $this->tablestore->getRow([
            'table_name' => $this->table,
            'primary_key' => [
                [$this->keyAttribute, $key, PrimaryKeyTypeConst::CONST_STRING],
            ],
            'max_versions' => 1,
        ]);

        $column = $response['attribute_columns'][0] ?? [];

        if (! $column) {
            return;
        }

        if ($this->isExpired($column)) {
            return;
        }

        return $column[$this->columnValueIndex] ?? null;
    }

    public function many(array $keys)
    {
        // TODO: Implement many() method.
    }

    /**
     * Store an item in the cache for a given number of seconds.
     *
     * @param  string  $key
     * @param  mixed  $value
     * @param  int  $seconds
     * @return bool
     */
    public function put($key, $value, $seconds)
    {
        $this->tablestore->putRow([
            'table_name' => $this->table,
            'primary_key' => [
                [$this->keyAttribute, $key, PrimaryKeyTypeConst::CONST_STRING],
            ],
            'attribute_columns' => [
                [$this->valueAttribute, $value, ColumnTypeConst::CONST_STRING, $this->toTimestamp($seconds)],
            ],
        ]);

        return true;
    }

    public function putMany(array $values, $seconds)
    {
        $expiration = $this->toTimestamp($seconds);

        $this->tablestore->batchWriteRow([
            //
        ]);

        return true;
    }

    public function increment($key, $value = 1)
    {
        // TODO: Implement increment() method.
    }

    public function decrement($key, $value = 1)
    {
        // TODO: Implement decrement() method.
    }

    public function forever($key, $value)
    {
        // TODO: Implement forever() method.
    }

    public function forget($key)
    {
        // TODO: Implement forget() method.
    }

    public function flush()
    {
        // TODO: Implement flush() method.
    }

    public function getPrefix()
    {
        // TODO: Implement getPrefix() method.
    }

    protected function toTimestamp(int $seconds): int
    {
        $timestamp = $seconds > 0
            ? $this->availableAt($seconds)
            : Carbon::now()->getTimestamp();

        return $timestamp * 1000;
    }

    protected function isExpired(array $column, DateTimeInterface $expiration = null): bool
    {
        $expiration = $expiration ?: Carbon::now();

        return isset($column[$this->columnTimestampIndex]) &&
            ($expiration->getTimestamp() * 1000) >= $column[$this->columnTimestampIndex];
    }
}
