# logstash-input-ks3
logstash input插件，实现从金山云ks3中同步读取数据

## 使用方式

### 安装插件

进入logstash的解压目录，执行：

```
./bin/logstash-plugin install /logstash-input-ks3/logstash-input-ks3-0.0.1-java.gem
```
执行结果为：

```
Validating /usr/local/githome/logstash-input-ks3/logstash-input-ks3-0.0.1-java.gem
Installing logstash-input-ks3
Installation successful
```

### 编写配置文件
编写配置文件ks3.logstash.conf

```
input {
    ks3 {
        "endpoint" => "ks3-cn-beijing.ksyun.com" # 金山云ks3访问域名
        "access_key_id" => "*****" # 金山云ks3 ak
        "access_key_secret" => "****" # 金山云ks3 sk
        "bucket" => "******" # 金山云ks3 bucket
        "prefix" => "abc"
        "marker_file" => "."
        "interval" => 60 # 数据同步时间间隔，每60s拉取一次数据
    }
}

output {
    elasticsearch {
    hosts => ["http://172.16.0.39:9200"] # ES endpoint地址
    index => "access.log" # 索引
 }
}
```

### 执行logstash

```
./bin/logstash -f ks3.logstash.conf
```

## 引用
* [logstash-input-cos](https://github.com/gaobinlong/logstash-input-cos)
* [logstash-input-example](https://github.com/logstash-plugins/logstash-input-example)
