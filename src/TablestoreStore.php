<?php

namespace Zhineng\Tablestore;

use DateTimeInterface;
use Dew\Tablestore\Attribute;
use Dew\Tablestore\Exceptions\TablestoreException;
use Dew\Tablestore\PlainbufferWriter;
use Dew\Tablestore\PrimaryKey;
use Dew\Tablestore\Responses\RowDecodableResponse;
use Dew\Tablestore\Tablestore;
use Illuminate\Contracts\Cache\Lock;
use Illuminate\Contracts\Cache\Store;
use Illuminate\Support\Carbon;
use Illuminate\Support\InteractsWithTime;
use Protos\ComparatorType;
use Protos\Filter;
use Protos\FilterType;
use Protos\SingleColumnValueFilter;
use RuntimeException;

class TablestoreStore implements Store
{
    use InteractsWithTime;

    /**
     * Create a Tablestore cache store.
     */
    public function __construct(
        protected Tablestore $tablestore,
        protected string $table,
        protected string $keyAttribute = 'key',
        protected string $valueAttribute = 'value',
        protected string $expirationAttribute = 'expires_at',
        protected string $prefix = ''
    ) {
        $this->setPrefix($prefix);
    }

    /**
     * Retrieve an item from the cache by key.
     *
     * @param  string|array  $key
     * @return mixed
     */
    public function get($key)
    {
        $response = $this->tablestore->table($this->table)
            ->whereKey($this->keyAttribute, $this->prefix.$key)
            ->where($this->expirationAttribute, '>', Carbon::now()->getTimestampMs())
            ->get();

        $item = $response->getDecodedRow();

        if ($item === null) {
            return;
        }

        if (isset($item[$this->valueAttribute])) {
            return $this->unserialize(
                $item[$this->valueAttribute][0]->value()
            );
        }
    }

    /**
     * Retrieve multiple items from the cache by key.
     *
     * Items not found in the cache will have a null value.
     *
     * @param  array  $keys
     * @return array
     */
    public function many(array $keys): array
    {
        $now = Carbon::now()->getTimestampMs();

        $response = $this->tablestore->batch(function ($query) use ($keys, $now) {
            foreach ($keys as $key) {
                $query->table($this->table)
                    ->whereKey($this->keyAttribute, $this->prefix.$key)
                    ->where($this->expirationAttribute, '>', $now)
                    ->get();
            }
        });

        $prefix = strlen($this->prefix);
        $result = array_fill_keys($keys, null);

        foreach ($response->getTables()[0]->getRows() as $row) {
            $decoded = (new RowDecodableResponse($row))->getDecodedRow();

            if ($decoded === null) {
                continue;
            }

            $key = substr($decoded[$this->keyAttribute]->value(), $prefix);

            $result[$key] = $this->unserialize($decoded[$this->valueAttribute][0]->value());
        }

        return $result;
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
        $this->tablestore->table($this->table)->insert([
            PrimaryKey::string($this->keyAttribute, $this->prefix.$key),
            Attribute::createFromValue($this->valueAttribute, $this->serialize($value)),
            Attribute::integer($this->expirationAttribute, $this->toTimestamp($seconds)),
        ]);

        return true;
    }

    /**
     * Store multiple items in the cache for a given number of seconds.
     *
     * @param  array  $values
     * @param  int  $seconds
     * @return bool
     */
    public function putMany(array $values, $seconds)
    {
        $expiration = $this->toTimestamp($seconds);

        $this->tablestore->batch(function ($query) use ($values, $expiration) {
            foreach ($values as $key => $value) {
                $query->table($this->table)->insert([
                    PrimaryKey::string($this->keyAttribute, $this->prefix.$key),
                    Attribute::createFromValue($this->valueAttribute, $this->serialize($value)),
                    Attribute::integer($this->expirationAttribute, $expiration),
                ]);
            }
        });

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
            Attribute::integer($this->expirationAttribute, Carbon::now()->getTimestampMs())
                ->toFormattedValue($now = new PlainbufferWriter);

            // Include only items that do not exist or that have expired
            // expression: expiration <= now
            $filter = new Filter;
            $filter->setType(FilterType::FT_SINGLE_COLUMN_VALUE);
            $filter->setFilter((new SingleColumnValueFilter)
                ->setColumnName($this->expirationAttribute)
                ->setComparator(ComparatorType::CT_LESS_THAN)
                ->setColumnValue($now->getBuffer())
                ->setFilterIfMissing(false) // allow missing
                ->setLatestVersionOnly(true)
                ->serializeToString());

            $this->tablestore->table($this->table)
                ->ignoreExistence()
                ->where($filter)
                ->insert([
                    PrimaryKey::string($this->keyAttribute, $this->prefix.$key),
                    Attribute::createFromValue($this->valueAttribute, $this->serialize($value)),
                    Attribute::integer($this->expirationAttribute, $this->toTimestamp($seconds)),
                ]);
        } catch (TablestoreException $e) {
            if ($e->getError()->getCode() === 'OTSConditionCheckFail') {
                return false;
            }

            throw $e;
        }

        return true;
    }

    /**
     * Increment the value of an item in the cache.
     *
     * @param  string  $key
     * @param  mixed  $value
     * @return int|bool
     */
    public function increment($key, $value = 1)
    {
        try {
            $this->tablestore->table($this->table)
                ->whereKey($this->keyAttribute, $this->prefix.$key)
                ->expectExists()
                ->update([
                    Attribute::increment($this->valueAttribute, $value),
                ]);

            return true;
        } catch (TablestoreException $e) {
            if ($e->getError()->getCode() === 'OTSConditionCheckFail') {
                return false;
            }

            throw $e;
        }
    }

    /**
     * Decrement the value of an item in the cache.
     *
     * @param  string  $key
     * @param  mixed  $value
     * @return int|bool
     */
    public function decrement($key, $value = 1)
    {
        try {
            $this->tablestore->table($this->table)
                ->whereKey($this->keyAttribute, $this->prefix.$key)
                ->expectExists()
                ->update([
                    Attribute::decrement($this->valueAttribute, $value),
                ]);

            return true;
        } catch (TablestoreException $e) {
            if ($e->getError()->getCode() === 'ConditionalCheckFailed') {
                return false;
            }

            throw $e;
        }
    }

    /**
     * Store an item in the cache indefinitely.
     *
     * @param  string  $key
     * @param  mixed  $value
     * @return bool
     */
    public function forever($key, $value)
    {
        return $this->put($key, $value, Carbon::now()->addYears(5)->getTimestampMs());
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
     * Remove an item from the cache.
     *
     * @param  string  $key
     * @return bool
     */
    public function forget($key)
    {
        $this->tablestore->table($this->table)
            ->whereKey($this->keyAttribute, $this->prefix.$key)
            ->delete();

        return true;
    }

    /**
     * Remove all items from the cache.
     *
     * @return bool
     */
    public function flush()
    {
        throw new RuntimeException('Tablestore does not support flushing an entire table. Please create a new table.');
    }

    /**
     * Set the cache key prefix.
     *
     * @param  string  $prefix
     * @return void
     */
    protected function setPrefix(string $prefix): void
    {
        $this->prefix = $prefix === '' ? '' : $prefix.':';
    }

    /**
     * Get the cache key prefix.
     *
     * @return string
     */
    public function getPrefix()
    {
        return $this->prefix;
    }

    /**
     * Generate a storable representation of a value.
     */
    protected function serialize($value): int|float|bool|string
    {
        return match(gettype($value)) {
            'integer', 'double', 'boolean' => $value,
            default => serialize($value),
        };
    }

    /**
     * Create a PHP value from a stored representation.
     */
    protected function unserialize($value): mixed
    {
        return match (gettype($value)) {
            'integer', 'double', 'boolean' => $value,
            default => unserialize($value),
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
     * The underlying Tablestore client.
     */
    public function getClient(): Tablestore
    {
        return $this->tablestore;
    }
}
