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

    public function test_items_can_be_stored_and_retrieved()
    {
        Cache::driver('tablestore')->put('name', 'Zhineng', 10);
        Cache::driver('tablestore')->put('user', ['name' => 'Zhineng'], 10);
        Cache::driver('tablestore')->put('fahrenheit', 79, 10);
        Cache::driver('tablestore')->put('celsius', 26.5, 10);
        Cache::driver('tablestore')->put('string', '100', 10);
        Cache::driver('tablestore')->put('true', true, 10);
        Cache::driver('tablestore')->put('false', false, 10);
        Cache::driver('tablestore')->put('null', null, 10);

        $this->assertSame('Zhineng', Cache::driver('tablestore')->get('name'));
        $this->assertSame(79, Cache::driver('tablestore')->get('fahrenheit'));
        $this->assertSame(26.5, Cache::driver('tablestore')->get('celsius'));
        $this->assertSame(['name' => 'Zhineng'], Cache::driver('tablestore')->get('user'));
        $this->assertSame('100', Cache::driver('tablestore')->get('string'));
        $this->assertSame(true, Cache::driver('tablestore')->get('true'));
        $this->assertSame(false, Cache::driver('tablestore')->get('false'));
        $this->assertSame(null, Cache::driver('tablestore')->get('null'));
    }

    public function test_items_can_be_atomically_added()
    {
        $key = Str::random(6);

        $this->assertTrue(Cache::driver('tablestore')->add($key, 'Zhineng', 10));
        $this->assertFalse(Cache::driver('tablestore')->add($key, 'Zhineng', 10));
    }

    public function test_items_can_be_incremented_and_decremented()
    {
        Cache::driver('tablestore')->put('counter', 0, 10);
        Cache::driver('tablestore')->increment('counter');
        Cache::driver('tablestore')->increment('counter', 4);

        $this->assertSame(5, Cache::driver('tablestore')->get('counter'));

        Cache::driver('tablestore')->decrement('counter', 5);
        $this->assertSame(0, Cache::driver('tablestore')->get('counter'));
    }

    public function test_locks_can_be_acquired()
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
