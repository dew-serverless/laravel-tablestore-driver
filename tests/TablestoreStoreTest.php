<?php

namespace Zhineng\Tablestore\Tests;

use Illuminate\Support\Facades\Cache;
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
