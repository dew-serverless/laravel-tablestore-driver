<?php

namespace Zhineng\Tablestore;

use AlibabaCloud\Client\Clients\AccessKeyClient;
use Aliyun\OTS\OTSClient;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\ServiceProvider;

class TablestoreServiceProvider extends ServiceProvider
{
    public function register()
    {
        $this->app->booting(function () {
            Cache::extend('tablestore', function ($app, $config) {
                return Cache::repository(
                    new TablestoreStore(
                        new OTSClient([
                            'EndPoint' => $config['endpoint'],
                            'AccessKeyID' => $config['key'],
                            'AccessKeySecret' => $config['secret'],
                            'InstanceName' => $config['instance'],
                        ]),
                        $config['table']
                    )
                );
            });
        });
    }
}
