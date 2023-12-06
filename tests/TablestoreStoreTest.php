<?php

namespace Zhineng\Tablestore\Tests;

use Illuminate\Contracts\Config\Repository;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Str;
use Orchestra\Testbench\TestCase;
use Zhineng\Tablestore\TablestoreServiceProvider;

class TablestoreStoreTest extends TestCase
{
    /**
     * Setup the test environment.
     */
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

    public function test_items_can_be_stored_and_retrieved_in_batches()
    {
        $items = [
            'many-name' => 'Zhineng',
            'many-user' => ['name' => 'Zhineng'],
            'many-fahrenheit' => 79,
            'many-celsius' => 26.5,
            'many-string' => '100',
            'many-true' => true,
            'many-false' => false,
            'many-null' => null,
        ];

        Cache::driver('tablestore')->putMany($items, 10);
        $result = Cache::driver('tablestore')->many(array_keys($items));
        $this->assertSame($result, $items);
    }

    public function test_items_can_be_returned_early_if_keys_are_empty()
    {
        $this->assertSame([], Cache::driver('tablestore')->many([]));
    }

    public function test_items_can_be_returned_early_when_saving_if_values_are_empty()
    {
        $this->assertTrue(Cache::driver('tablestore')->putMany([], 10));
    }

    public function test_items_not_found_will_have_a_null_value()
    {
        $this->assertNull(Cache::driver('tablestore')->get('not-exists-1'));

        $this->assertSame([
            'not-exists-1' => null, 'not-exists-2' => null,
        ], Cache::driver('tablestore')->many(['not-exists-1', 'not-exists-2']));
    }

    public function test_expired_items_will_have_a_null_value()
    {
        Cache::driver('tablestore')->getStore()->put('expired', 'Zhineng', 0);
        $this->assertNull(Cache::driver('tablestore')->get('expired'));
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

    /**
     * Get package providers.
     *
     * @param  \Illuminate\Foundation\Application  $app
     * @return array<int, class-string<\Illuminate\Support\ServiceProvider>>
     */
    protected function getPackageProviders($app)
    {
        return [
            TablestoreServiceProvider::class,
        ];
    }

    /**
     * Define environment setup.
     *
     * @param  \Illuminate\Foundation\Application  $app
     * @return void
     */
    protected function defineEnvironment($app)
    {
        tap($app['config'], function (Repository $config) {
            $config->set('cache.stores.tablestore', [
                'driver' => 'tablestore',
                'key' => env('ALIYUN_ACCESS_KEY_ID'),
                'secret' => env('ALIYUN_SECRET_ACCESS_KEY'),
                'endpoint' => env('TABLESTORE_ENDPOINT'),
                'instance' => env('TABLESTORE_INSTANCE_NAME'),
                'table' => env('TABLESTORE_CACHE_TABLE', 'laravel_test'),
            ]);
        });
    }
}
