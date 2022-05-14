<?php

namespace Zhineng\Tablestore;

use Aliyun\OTS\OTSClient;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\ServiceProvider;

class TablestoreServiceProvider extends ServiceProvider
{
    public function register()
    {
        $this->app->booting(function () {
            Cache::extend('tablestore', function ($app, $config) {
                $client = new OTSClient([
                    'EndPoint' => $config['endpoint'],
                    'AccessKeyID' => $config['key'],
                    'AccessKeySecret' => $config['secret'],
                    'InstanceName' => $config['instance'],
                    'ErrorLogHandler' => '',
                    'DebugLogHandler' => '',
                ]);

                return Cache::repository(
                    new TablestoreStore(
                        $client,
                        $config['table'],
                        $config['attributes']['key'] ?? 'key',
                        $config['attributes']['value'] ?? 'value',
                        $config['attributes']['expiration'] ?? 'expires_at',
                        $config['prefix'] ?? $this->app['config']['cache.prefix']
                    )
                );
            });
        });
    }
}
