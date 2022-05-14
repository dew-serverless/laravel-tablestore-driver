<?php

namespace Zhineng\Tablestore;

use Aliyun\OTS\Consts\ColumnTypeConst;
use Aliyun\OTS\Consts\ComparatorTypeConst;
use Aliyun\OTS\Consts\OperationTypeConst;
use Aliyun\OTS\Consts\PrimaryKeyTypeConst;
use Aliyun\OTS\Consts\RowExistenceExpectationConst;
use Aliyun\OTS\OTSClient;
use Aliyun\OTS\OTSServerException;
use DateTimeInterface;
use Illuminate\Contracts\Cache\Lock;
use Illuminate\Contracts\Cache\Store;
use Illuminate\Support\Carbon;
use Illuminate\Support\InteractsWithTime;
use Illuminate\Support\Str;
use RuntimeException;

class TablestoreStore implements Store
{
    use InteractsWithTime;

    public function __construct(
        protected OTSClient $tablestore,
        protected string $table,
        protected string $keyAttribute = 'key',
        protected string $valueAttribute = 'value',
        protected string $expirationAttribute = 'expires_at',
        protected string $prefix = ''
    ) {
        $this->setPrefix($prefix);
    }

    /**
     * @inheritDoc
     */
    public function get($key)
    {
        $response = $this->tablestore->getRow([
            'table_name' => $this->table,
            'primary_key' => [
                [$this->keyAttribute, $this->prefix.$key, PrimaryKeyTypeConst::CONST_STRING],
            ],
            'max_versions' => 1,
        ]);

        if (! isset($response['attribute_columns'])) {
            return;
        }

        $item = $this->itemFromColumns($response['attribute_columns']);

        if ($this->isExpired($item)) {
            return;
        }

        if (isset($item[$this->valueAttribute])) {
            return $this->unserialize(
                $item[$this->valueAttribute][ColumnStructure::VALUE_INDEX],
                $item[$this->valueAttribute][ColumnStructure::TYPE_INDEX]
            );
        }
    }

    /**
     * @inheritDoc
     */
    public function many(array $keys): array
    {
        $prefixedKeys = array_map(function ($key) {
            return $this->getPrefix().$key;
        }, $keys);

        $response = $this->tablestore->batchGetRow([
            'tables' => [
                [
                    'table_name' => $this->table,
                    'primary_keys' => collect($prefixedKeys)->map(function ($key) {
                        return [
                            [$this->keyAttribute, $this->getPrefix().$key, PrimaryKeyTypeConst::CONST_STRING],
                        ];
                    })->all(),
                    'max_versions' => 1,
                ],
            ],
        ]);

        $now = Carbon::now();

        return array_merge(collect(array_flip($keys))->map(function () {
            //
        })->all(), collect($response['tables'][0]['rows'])->mapWithKeys(function ($response) use ($now) {
            // filter out the empty response
            if (empty($response['attribute_columns'])) {
                return [];
            }

            $item = $this->itemFromColumns($response['attribute_columns']);

            if ($this->isExpired($item, $now)) {
                $value = null;
            } else {
                $value = $item[$this->valueAttribute];
            }

            return [
                Str::replaceFirst($this->getPrefix(), '',
                    $response['primary_key'][0][PrimaryKeyStructure::VALUE_INDEX]) => $value,
            ];
        })->all());
    }

    /**
     * @inheritDoc
     */
    public function put($key, $value, $seconds)
    {
        $this->tablestore->putRow([
            'table_name' => $this->table,
            'primary_key' => [
                [$this->keyAttribute, $this->prefix.$key, PrimaryKeyTypeConst::CONST_STRING],
            ],
            'attribute_columns' => [
                [$this->valueAttribute, $this->serialize($value), $this->type($value)],
                [$this->expirationAttribute, $this->toTimestamp($seconds), ColumnTypeConst::CONST_INTEGER],
            ],
        ]);

        return true;
    }

    /**
     * @inheritDoc
     */
    public function putMany(array $values, $seconds)
    {
        $expiration = $this->toTimestamp($seconds);

        $this->tablestore->batchWriteRow([
            'tables' => [
                [
                    'table_name' => $this->table,
                    'rows' => collect($values)->map(function ($value, $key) use ($expiration) {
                        return [
                            'operation_type' => OperationTypeConst::CONST_PUT,
                            'condition' => RowExistenceExpectationConst::CONST_IGNORE,
                            'primary_key' => [
                                [$this->keyAttribute, $this->getPrefix().$key, PrimaryKeyTypeConst::CONST_STRING],
                            ],
                            'attribute_columns' => [
                                [$this->valueAttribute, $value, $this->type($value), $expiration],
                                [$this->expirationAttribute, $expiration, ColumnTypeConst::CONST_INTEGER, $expiration],
                            ],
                        ];
                    })->values()->all(),
                ],
            ],
        ]);

        return true;
    }

    /**
     * Store an item in the cache if the key doesn't exist.
     *
     * @param  string  $key
     * @param  mixed  $value
     * @param  int  $seconds
     * @return bool
     */
    public function add($key, $value, $seconds): bool
    {
        try {
            $this->tablestore->putRow([
                'table_name' => $this->table,
                'primary_key' => [
                    [$this->keyAttribute, $this->prefix.$key, PrimaryKeyTypeConst::CONST_STRING],
                ],
                'attribute_columns' => [
                    [$this->valueAttribute, $this->serialize($value), $this->type($value)],
                    [$this->expirationAttribute, $this->toTimestamp($seconds), ColumnTypeConst::CONST_INTEGER],
                ],
                'condition' => [
                    'row_existence' => RowExistenceExpectationConst::CONST_IGNORE,
                    'column_condition' => [
                        'column_name' => $this->expirationAttribute,
                        'value' => Carbon::now()->getTimestamp() * 1000,
                        'comparator' => ComparatorTypeConst::CONST_LESS_THAN,
                        'pass_if_missing' => true,
                    ],
                ],
            ]);
        } catch (OTSServerException $e) {
            if (Str::contains($e->getMessage(), 'OTSConditionCheckFail')) {
                return false;
            }

            throw $e;
        }

        return true;
    }

    /**
     * @inheritDoc
     */
    public function increment($key, $value = 1)
    {
        // We can't increment the value with single call due to the
        // SDK limitation, so take the current value out first.
        if (is_null($current = $this->get($key))) {
            return false;
        }

        try {
            $this->tablestore->updateRow([
                'table_name' => $this->table,
                'primary_key' => [
                    [$this->keyAttribute, $this->prefix.$key, PrimaryKeyTypeConst::CONST_STRING],
                ],
                'update_of_attribute_columns' => [
                    'PUT' => [
                        [$this->valueAttribute, $updated = $current + $value, ColumnTypeConst::CONST_INTEGER],
                    ],
                ],
                'condition' => [
                    'row_existence' => RowExistenceExpectationConst::CONST_EXPECT_EXIST,
                    'column_condition' => [
                        'column_name' => $this->valueAttribute,
                        'value' => $current,
                        'comparator' => ComparatorTypeConst::CONST_EQUAL,
                        'pass_if_missing' => false,
                    ],
                ],
            ]);

            return $updated;
        } catch (OTSServerException $e) {
            if (Str::contains($e->getMessage(), 'OTSConditionCheckFail')) {
                return false;
            }

            throw $e;
        }
    }

    /**
     * @inheritDoc
     */
    public function decrement($key, $value = 1)
    {
        // We can't decrement the value with single call due to the
        // SDK limitation, so take the current value out first.
        if (is_null($current = $this->get($key))) {
            return false;
        }

        try {
            $this->tablestore->updateRow([
                'table_name' => $this->table,
                'primary_key' => [
                    [$this->keyAttribute, $this->prefix.$key, PrimaryKeyTypeConst::CONST_STRING],
                ],
                'update_of_attribute_columns' => [
                    'PUT' => [
                        [$this->valueAttribute, $updated = $current - $value, ColumnTypeConst::CONST_INTEGER],
                    ],
                ],
                'condition' => [
                    'row_existence' => RowExistenceExpectationConst::CONST_EXPECT_EXIST,
                    'column_condition' => [
                        'column_name' => $this->valueAttribute,
                        'value' => $current,
                        'comparator' => ComparatorTypeConst::CONST_EQUAL,
                        'pass_if_missing' => false,
                    ],
                ],
            ]);

            return $updated;
        } catch (OTSServerException $e) {
            if (Str::contains($e->getMessage(), 'ConditionalCheckFailed')) {
                return false;
            }

            throw $e;
        }
    }

    /**
     * @inheritDoc
     */
    public function forever($key, $value)
    {
        return $this->put($key, $value, Carbon::now()->addYears(5)->getTimestamp() * 1000);
    }

    /**
     * Get a lock instance.
     *
     * @param  string  $name
     * @param  int  $seconds
     * @param  string|null  $owner
     * @return \Illuminate\Contracts\Cache\Lock
     */
    public function lock($name, $seconds = 0, $owner = null): Lock
    {
        return new TablestoreLock($this, $this->prefix.$name, $seconds, $owner);
    }

    /**
     * @inheritDoc
     */
    public function forget($key): bool
    {
        $this->tablestore->deleteRow([
            'table_name' => $this->table,
            'condition' => RowExistenceExpectationConst::CONST_IGNORE,
            'primary_key' => [
                [$this->keyAttribute, $this->getPrefix().$key, PrimaryKeyTypeConst::CONST_STRING],
            ],
        ]);

        return true;
    }

    /**
     * @inheritDoc
     */
    public function flush()
    {
        throw new RuntimeException('Tablestore does not support flushing an entire table. Please create a new table.');
    }

    /**
     * Build up the structured data item for the response.
     *
     * @param  array  $columns
     * @return array
     */
    protected function itemFromColumns(array $columns): array
    {
        return collect($columns)->mapWithKeys(function ($column) {
            return [$column[ColumnStructure::NAME_INDEX] => $column];
        })->toArray();
    }

    /**
     * Set the cache key prefix.
     *
     * @param  string  $prefix
     * @return void
     */
    protected function setPrefix(string $prefix): void
    {
        $this->prefix = ! empty($prefix) ? $prefix.':' : '';
    }

    /**
     * @inheritDoc
     */
    public function getPrefix(): string
    {
        return $this->prefix;
    }

    protected function serialize($value): string
    {
        return match(gettype($value)) {
            'integer', 'double', 'boolean' => (string) $value,
            default => serialize($value),
        };
    }

    protected function unserialize(mixed $value, string $type): mixed
    {
        return match ($type) {
            ColumnTypeConst::CONST_INTEGER => (int) $value,
            ColumnTypeConst::CONST_DOUBLE => (float) $value,
            ColumnTypeConst::CONST_BOOLEAN => $value,
            ColumnTypeConst::CONST_STRING => unserialize($value),
        };
    }

    /**
     * Get the Tablestore type for the given value.
     *
     * @param  mixed  $value
     * @return string
     */
    protected function type(mixed $value): string
    {
        return match (gettype($value)) {
            'integer' => ColumnTypeConst::CONST_INTEGER,
            'double' => ColumnTypeConst::CONST_DOUBLE,
            'boolean' => ColumnTypeConst::CONST_BOOLEAN,
            default => ColumnTypeConst::CONST_STRING,
        };
    }

    /**
     * Get the UNIX timestamp in milliseconds for the given number of seconds.
     *
     * @param  int  $seconds
     * @return int
     */
    protected function toTimestamp(int $seconds): int
    {
        $timestamp = $seconds > 0
            ? $this->availableAt($seconds)
            : Carbon::now()->getTimestamp();

        return $timestamp * 1000;
    }

    /**
     * Determine if the given item is expired.
     *
     * @param  array  $item
     * @param  DateTimeInterface|null  $expiration
     * @return bool
     */
    protected function isExpired(array $item, DateTimeInterface $expiration = null): bool
    {
        $expiration = $expiration ?: Carbon::now();

        return isset($item[$this->expirationAttribute][ColumnStructure::VALUE_INDEX]) &&
            ($expiration->getTimestamp() * 1000) >= $item[$this->expirationAttribute][ColumnStructure::VALUE_INDEX];
    }

    public function getClient(): OTSClient
    {
        return $this->tablestore;
    }
}
