<?php

namespace Zhineng\Tablestore;

use Illuminate\Cache\Lock;

class TablestoreLock extends Lock
{
    public function __construct(
        protected TablestoreStore $tablestore,
        string $name,
        int $seconds,
        ?string $owner = null
    )
    {
        parent::__construct($name, $seconds, $owner);
    }

    /**
     * @inheritDoc
     */
    public function acquire()
    {
        return $this->tablestore->add(
            $this->name, $this->owner, $this->seconds
        );
    }

    /**
     * @inheritDoc
     */
    public function release()
    {
        if ($this->isOwnedByCurrentProcess()) {
            return $this->tablestore->forget($this->name);
        }

        return false;
    }

    /**
     * @inheritDoc
     */
    public function forceRelease()
    {
        $this->tablestore->forget($this->name);
    }

    /**
     * @inheritDoc
     */
    protected function getCurrentOwner()
    {
        return $this->tablestore->get($this->name);
    }
}
