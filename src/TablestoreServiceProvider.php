<?php

declare(strict_types=1);

namespace Zhineng\Tablestore;

use Dew\Tablestore\Tablestore;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\ServiceProvider;

final class TablestoreServiceProvider extends ServiceProvider
{
    /**
     * Register any application services.
     *
     * @return void
     */
    public function register()
    {
        $this->app->booting(function () {
            Cache::extend('tablestore', function ($app, $config) {
                $client = new Tablestore(
                    $config['key'], $config['secret'],
                    $config['endpoint'], $config['instance'] ?? null
                );

                return Cache::repository(new TablestoreStore(
                    $client,
                    $config['table'],
                    $config['attributes']['key'] ?? 'key',
                    $config['attributes']['value'] ?? 'value',
                    $config['attributes']['expiration'] ?? 'expires_at',
                    $config['prefix'] ?? $this->app['config']['cache.prefix']
                ));
            });
        });
    }
}
