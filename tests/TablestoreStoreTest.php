<?php

namespace Zhineng\Tablestore\Tests;

use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Str;
use Orchestra\Testbench\TestCase;
use Zhineng\Tablestore\TablestoreServiceProvider;

class TablestoreStoreTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        if (! env('TABLESTORE_CACHE_TABLE')) {
            $this->markTestSkipped('Tablestore not configured.');
        }
    }

    public function testItemsCanBeStoredAndRetrieved()
    {
        Cache::driver('tablestore')->put('name', 'Zhineng', 10);
        $this->assertSame('Zhineng', Cache::driver('tablestore')->get('name'));

        Cache::driver('tablestore')->put(['name' => 'Shiyun', 'age' => 28], 10);
        $this->assertSame('Shiyun', Cache::driver('tablestore')->get('name'));
        $this->assertEquals(28, Cache::driver('tablestore')->get('age'));

        $this->assertEquals([
            'name' => 'Shiyun',
            'age' => 28,
            'height' => null,
        ], Cache::driver('tablestore')->many(['name', 'age', 'height']));

        Cache::driver('tablestore')->forget('name');
        $this->assertNull(Cache::driver('tablestore')->get('name'));
    }

    public function testItemsCanBeAtomicallyAdded()
    {
        $key = Str::random(6);

        $this->assertTrue(Cache::driver('tablestore')->add($key, 'Zhineng', 10));
        $this->assertFalse(Cache::driver('tablestore')->add($key, 'Zhineng', 10));
    }

    public function testItemsCanBeIncrementedAndDecremented()
    {
        Cache::driver('tablestore')->put('counter', 0, 10);
        Cache::driver('tablestore')->increment('counter');
        Cache::driver('tablestore')->increment('counter', 4);

        $this->assertEquals(5, Cache::driver('tablestore')->get('counter'));

        Cache::driver('tablestore')->decrement('counter', 5);
        $this->assertEquals(0, Cache::driver('tablestore')->get('counter'));
    }

    public function testLocksCanBeAcquired()
    {
        Cache::driver('tablestore')->lock('lock', 10)->get(function () {
            $this->assertFalse(Cache::driver('tablestore')->lock('lock', 10)->get());
        });
    }

    protected function getPackageProviders($app)
    {
        return [TablestoreServiceProvider::class];
    }

    protected function getEnvironmentSetUp($app)
    {
        $app['config']->set('cache.default', 'tablestore');

        $app['config']->set('cache.stores.tablestore', [
            'driver' => 'tablestore',
            'key' => env('ALIYUN_ACCESS_KEY_ID'),
            'secret' => env('ALIYUN_SECRET_ACCESS_KEY'),
            'region' => 'cn-hongkong',
            'instance' => env('TABLESTORE_INSTANCE_NAME'),
            'table' => env('TABLESTORE_CACHE_TABLE', 'laravel_test'),
            'endpoint' => env('TABLESTORE_ENDPOINT'),
        ]);
    }
}
