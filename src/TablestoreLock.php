<?php

declare(strict_types=1);

namespace Zhineng\Tablestore;

use Illuminate\Cache\Lock;
use RuntimeException;

final class TablestoreLock extends Lock
{
    /**
     * Create a new Tablestore lock instance.
     *
     * @param  string  $name
     * @param  int  $seconds
     * @param  string|null  $owner
     * @return void
     */
    public function __construct(
        protected TablestoreStore $tablestore,
        $name, $seconds, $owner = null
    ) {
        parent::__construct($name, $seconds, $owner);
    }

    /**
     * Attempt to acquire the lock.
     *
     * @return bool
     */
    public function acquire()
    {
        return $this->tablestore->add(
            $this->name, $this->owner, $this->seconds
        );
    }

    /**
     * Release the lock.
     *
     * @return bool
     */
    public function release()
    {
        if ($this->isOwnedByCurrentProcess()) {
            return $this->tablestore->forget($this->name);
        }

        return false;
    }

    /**
     * Releases this lock in disregard of ownership.
     *
     * @return void
     */
    public function forceRelease()
    {
        $this->tablestore->forget($this->name);
    }

    /**
     * Returns the owner value written into the driver for this lock.
     *
     * @return string
     */
    protected function getCurrentOwner()
    {
        $owner = $this->tablestore->get($this->name);

        if (is_string($owner)) {
            return $owner;
        }

        throw new RuntimeException('The owner seems to be modified somewhere else.');
    }
}
