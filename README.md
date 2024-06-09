[toc]

### Performance test and Migration Tools for some DB Engine(e.g. AnalyticDB PostgreSQL).

#### 1. 程序

1. adb-mysql 目录下都是 AnalyticDB MySQL 相关的工具

   1.1. adb-mysql-read: AnalyticDB MySQL 多线程读性能的测试

   1.2. adb-mysql-write: AnalyticDB MySQL 多线程读性能的测试

    1.3. adb-mysql-unload: AnalyticDB MySQL 将所有表数据导出，压缩上传到 OSS，并写消息到 Amazon SQS, 提醒从 AWS Lambda 下载文件写入 S3


2. adb-pg 目录下都是 AnalyticDB PostgreSQL 相关的工具

    2.1. adb-pg-read: AnalyticDB PostgreSQL 多线程读性能的测试

    2.2. adb-pg-write: AnalyticDB PostgreSQL 多线程读性能的测试

    2.3. adb-pg-unload: AnalyticDB PostgreSQL 将所有表数据导出，压缩上传到 OSS，并写消息到 Amazon SQS, 提醒从 AWS Lambda 下载文件写入 S3


#### 2. 环境 

```markdown
*  Python 3.9, 3.10
```

#### 3. 准备 & 运行

##### 3.2 运行前准备

创建一台阿里云 ECS, 确保可以连接到数据库，我们的代码是用 Python 编写的，需要安装 Python 和依赖包。

```properties
sudo yum install -y gcc python3.9 python-devel python-pip libpq-devel* postgresql-server
sudo pip3 install --upgrade pip
sudo pip3 install PyMysql psycopg2-binary boto3
```

配置操作 OSS 的工具 ossutil
```properties
sudo yum install -y unzip
sudo -v ; curl https://gosspublic.alicdn.com/ossutil/install.sh | sudo bash
```

配置 ossutil, 输入
```properties
ossutil config
# 需要根据提示符输入 accessKeyID, accessKeySecret, endpoint, 其余内容选择默认
# endpoint : oss-cn-hangzhou-internal.aliyuncs.com
```


##### 3.2 运行程序说明

以 adb-mysql 为例。

###### 3.2.1 adb-mysql-read.py

* 支持参数

```properties
参数 1: 并发线程数
参数 2: 每个线程读多次数据库
```

* 启动样例

```shell
python3 redshift-read-1.py 100 10000
```

###### 3.2.2 adb-mysql-write.py

* 支持参数

```properties
参数 1: 并发线程数
参数 2: 每个线程写多次数据库
```

* 启动样例

```shell
python3 adb-mysql-write.py 100 100000
```

###### 3.2.3 adb-mysql-unload.py

* 支持参数

```properties
参数 1: AnalyticDB Host
参数 2: AnalyticDB Database
参数 3: AnalyticDB Schema
参数 4: AnalyticDB User
参数 5: AnalyticDB Password
参数 6: 导出到 Oss 的目录
参数 7: 导出到本地的目录
参数 8: AWS Access Key, 用于发送消息到 AWS SQS
参数 9: AWS Secret Key, 用于发送消息到 AWS SQS
参数 10: AWS Region, 用于发送消息到 AWS SQS
参数 11: AWS SQS URL, 用于发送消息到 AWS SQS
```

* 启动样例

```shell
python3 adb-mysql-unload.py gp-bp10cn2866q7t90ub-master.gpdb.rds.aliyuncs.com adb_sampledata_tpch \
        public postgres Password****** oss://dalei-demo/cdn /home/ecs-user \
        AccessKey****** SecretKey****** \
        us-east-1 https://sqs.us-east-1.amazonaws.com/551831295244/oss-2-s3
```
