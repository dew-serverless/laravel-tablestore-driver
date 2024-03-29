<?php

declare(strict_types=1);

namespace Dew\TablestoreDriver;

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
            $this->registerCacheDriver();
            $this->registerSessionDriver();
        });
    }

    /**
     * Register the Tablestore cache driver.
     */
    private function registerCacheDriver(): void
    {
        Cache::extend('tablestore', function ($app, $config) {
            $client = new Tablestore(
                $config['key'], $config['secret'],
                $config['endpoint'], $config['instance'] ?? null
            );

            if (isset($config['token'])) {
                $client->tokenUsing($config['token']);
            }

            if (isset($config['http'])) {
                $client->optionsUsing($config['http']);
            }

            return Cache::repository(
                new TablestoreStore(
                    $client,
                    $config['table'],
                    $config['attributes']['key'] ?? 'key',
                    $config['attributes']['value'] ?? 'value',
                    $config['attributes']['expiration'] ?? 'expires_at',
                    $this->getPrefix($config)
                )
            );
        });
    }

    /**
     * Register the Tablestore session driver.
     */
    private function registerSessionDriver(): void
    {
        /** @var \Illuminate\Session\SessionManager */
        $manager = $this->app->make('session');

        $handler = fn ($app) => $this->createCacheHandler('tablestore');

        $manager->extend('tablestore', $handler->bindTo($manager, $manager));
    }
}
