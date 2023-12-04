<?php

namespace Zhineng\Tablestore;

use Illuminate\Cache\Lock;

class TablestoreLock extends Lock
{
    /**
     * Create a Tablestore lock.
     */
    public function __construct(
        protected TablestoreStore $tablestore,
        string $name, int $seconds, string $owner = null
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
        return $this->tablestore->get($this->name);
    }
}
