
# 节点mempool实时同步程序

本程序通过监听节点zmq，实时获取tx内容并更新到redis、clickhouse中。可提供实时查询tx、余额、UTXO数据。

## 运行依赖

1. 需要节点开启zmq服务，至少启用rawtx队列。
2. 需要节点提供rpc服务。以便程序启动时初始化mempool。
3. 需要与satoblock服务使用同一个redis实例，同一个clickhouse实例。以便共享数据。

## 配置文件

在conf目录有程序运行需要的多个配置文件。

* db.yaml

clickhouse数据库配置，主要包括address、database等。

* chain.yaml

节点配置，主要包括zmq地址、rpc账号。

* redis.yaml

redis配置，主要包括address、database等。

* log.yaml

日志文件路径配置。

## 运行方式

直接启动程序即可。此时日志会直接输出到终端。

    $ ./satomempool

可使用nohup或其他技术将程序放置到后台运行。
