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
        protected string $expirationAttribute = 'expired_at',
        protected string $prefix = ''
    ) {
        $this->setPrefix($prefix);
    }

    /**
     * @inheritDoc
     */
    public function get($key)
    {
        return $this->getRow($key)[$this->valueAttribute] ?? null;
    }

    /**
     * Get the raw row for the given key.
     *
     * @param  string  $key
     * @return array|void
     * @throws OTSServerException
     * @throws \Aliyun\OTS\OTSClientException
     */
    protected function getRow($key)
    {
        $response = $this->tablestore->getRow([
            'table_name' => $this->table,
            'primary_key' => [
                [$this->keyAttribute, $this->getPrefix().$key, PrimaryKeyTypeConst::CONST_STRING],
            ],
            'max_versions' => 1,
        ]);

        if (empty($response['attribute_columns'])) {
            return;
        }

        $item = $this->buildItem($response['attribute_columns']);

        if ($this->isExpired($item)) {
            return;
        }

        return $item;
    }

    /**
     * @inheritDoc
     */
    public function many(array $keys): array
    {
        $prefixedKeys = array_map(function($key) {
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

            $item = $this->buildItem($response['attribute_columns']);

            if ($this->isExpired($item, $now)) {
                $value = null;
            } else {
                $value = $item[$this->valueAttribute];
            }

            return [Str::replaceFirst($this->getPrefix(), '', $response['primary_key'][0][PrimaryKeyStructure::VALUE_INDEX]) => $value];
        })->all());
    }

    /**
     * @inheritDoc
     */
    public function put($key, $value, $seconds)
    {
        $expiration = $this->toTimestamp($seconds);

        $this->tablestore->putRow([
            'table_name' => $this->table,
            'primary_key' => [
                [$this->keyAttribute, $this->getPrefix().$key, PrimaryKeyTypeConst::CONST_STRING],
            ],
            'attribute_columns' => [
                [$this->valueAttribute, $value, $this->type($value), $expiration],
                [$this->expirationAttribute, $expiration, ColumnTypeConst::CONST_INTEGER, $expiration],
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
                ]
            ],
        ]);

        return true;
    }

    /**
     * Store an item in the cache if the key does not exist.
     *
     * @param  string  $key
     * @param  mixed  $value
     * @param  \DateTimeInterface|\DateInterval|int|null  $ttl
     * @return bool
     */
    public function add($key, $value, $seconds): bool
    {
        try {
            $expiration = $this->toTimestamp($seconds);

            $this->tablestore->putRow([
                'table_name' => $this->table,
                'primary_key' => [
                    [$this->keyAttribute, $this->getPrefix().$key, PrimaryKeyTypeConst::CONST_STRING],
                ],
                'attribute_columns' => [
                    [$this->valueAttribute, $value, $this->type($value), $expiration],
                    [$this->expirationAttribute, $expiration, ColumnTypeConst::CONST_INTEGER, $expiration],
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
        // Due to the SDK limitation, we can't increment the value
        // by single call, so we need to take the old value out first.
        if (is_null($row = $this->getRow($key))) {
            return false;
        }

        try {
            $this->tablestore->updateRow([
                'table_name' => $this->table,
                'primary_key' => [
                    [$this->keyAttribute, $this->getPrefix().$key, PrimaryKeyTypeConst::CONST_STRING],
                ],
                'update_of_attribute_columns' => [
                    'PUT' => [
                        [
                            $this->valueAttribute, $row[$this->valueAttribute] + $value, ColumnTypeConst::CONST_INTEGER,
                            $row[$this->expirationAttribute]
                        ],
                    ],
                ],
                'condition' => [
                    'row_existence' => RowExistenceExpectationConst::CONST_EXPECT_EXIST,
                    'column_condition' => [
                        'column_name' => $this->valueAttribute,
                        'value' => $row[$this->valueAttribute],
                        'comparator' => ComparatorTypeConst::CONST_EQUAL,
                        'pass_if_missing' => false,
                    ],
                ],
            ]);

            return $row[$this->valueAttribute] + $value;
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
        // Due to the SDK limitation, we can't increment the value
        // by single call, so we need to take the old value out first.
        if (is_null($row = $this->getRow($key))) {
            return false;
        }

        try {
            $this->tablestore->updateRow([
                'table_name' => $this->table,
                'primary_key' => [
                    [$this->keyAttribute, $this->getPrefix().$key, PrimaryKeyTypeConst::CONST_STRING],
                ],
                'update_of_attribute_columns' => [
                    'PUT' => [
                        [
                            $this->valueAttribute, $row[$this->valueAttribute] - $value, ColumnTypeConst::CONST_INTEGER,
                            $row[$this->expirationAttribute]
                        ],
                    ],
                ],
                'condition' => [
                    'row_existence' => RowExistenceExpectationConst::CONST_EXPECT_EXIST,
                    'column_condition' => [
                        'column_name' => $this->valueAttribute,
                        'value' => $row[$this->valueAttribute],
                        'comparator' => ComparatorTypeConst::CONST_EQUAL,
                        'pass_if_missing' => false,
                    ],
                ],
            ]);

            return $row[$this->valueAttribute] - $value;
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
    protected function buildItem(array $columns): array
    {
        return collect($columns)->mapWithKeys(function ($column) {
            return [$column[ColumnStructure::NAME_INDEX] => $column[ColumnStructure::VALUE_INDEX]];
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

    /**
     * Get the Tablestore type for the given value.
     *
     * @param  mixed  $value
     * @return string
     */
    protected function type(mixed $value): string
    {
        return is_numeric($value)
            ? ColumnTypeConst::CONST_INTEGER
            : ColumnTypeConst::CONST_STRING;
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

        return isset($item[$this->expirationAttribute]) &&
            ($expiration->getTimestamp() * 1000) >= $item[$this->expirationAttribute];
    }
}
