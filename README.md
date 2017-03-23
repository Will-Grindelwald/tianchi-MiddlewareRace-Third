>写在前面: 
> 1. 在开始coding前请仔细阅读以下内容


## 1. 题目背景
支撑阿里双十一的万亿级消息中间件RocketMQ在去年10月进入Apache基金会进行孵化。异步解耦，削峰填谷，让消息中间件在现代软件架构中扮演者举足轻重的地位。天下大势，分久必合，合久必分，软件领域也是如此。市场上消息中间件种类繁多，阿里消息团队在此当口，推出厂商无关的Open-Messaging规范，促进消息领域的进一步发展。

## 2. 题目描述

### 2.1 题目内容
阅读Open-Messaging的接口代码(本工程内除了demo目录外的其他代码)，了解Topic，Queue的基本概念，并实现一个进程内消息引擎。

提示：Topic类似于水坝（蓄积功能，消峰填谷之利器），Queue类似于水渠；每当新建一个Queue的时候，可以选择绑定到几个Topic，类似于水渠从水坝引水；
每个Topic可以被任意多个Queue绑定，这点与现实生活不太一样，因为数据可以多次拷贝；
在发送的时候，可以选择发送到Topic，也可以选择直接发送到Queue；直接发送到Queue的数据只能被对应Queue消费，不能被其他Queue读取到；
在消费的时候，除了要读取绑定的Topic的数据，还要去取直接发送到该Queue的数据；


### 2.2 语言限定
限定使用JAVA语言

## 3.  程序目标

你的coding目标是实现以下接口:

 * Producer的createBytesMessageToTopic(topic, body)
 * Producer的createBytesMessageToQueue(queue, body)
 * Producer的send(message)
 * PullConsumer的attachQueue(queue, topics)
 * PullConsumer的pullNoWait()

实现类名字分别叫做:DefaultProducer,DefaultPullConsumer，包名均是：io.messaging.demo
请参考demo目录，建议是名字不要改，修改实现内容即可
注意，消息内容需要存入磁盘中，并能再次读取出来

## 4.参赛方法说明
1. 在阿里天池找到"中间件性能挑战赛"，并报名参加
2. 在code.aliyun.com注册一个账号，并新建一个仓库名，并将大赛官方账号middlewarerace添加为项目成员，权限为reporter
3. fork或者拷贝本仓库的代码到自己的仓库，并实现自己的逻辑
4. 在天池提交成绩的入口，提交自己的仓库git地址，等待评测结果
5. 坐等每天10点排名更新


## 4. 测试环境描述
测试环境为相同的4核虚拟机。限定使用的最大JVM大小为4GB，磁盘使用不做限制。

## 5. 程序校验逻辑

1. 10~20个线程各自独立调用Producer，发送消息，持续时间**T1**
2. 强行kill Producer进程，未持久化的消息都会丢失
3. 10~20个线程独立调用Consumer，每个Consumer消费指定的Queue，验证消息顺序准确性，可靠性，消费持续的时间为**T2**，消费到的总消息数目为**N**
4. 以**N/(t1+t2)**来衡量性能

    PS：请仔细阅读本仓库内Demo代码，尤其是DemoTester

# 6. 排名规则

在结果校验100%正确的前提下，按照N/(t1 + t2)从高到低来排名


# 7. 第二/三方库规约

* 仅允许依赖JavaSE 8 包含的lib
* 可以参考别人的实现，拷贝少量的代码
* 我们会对排名靠前的代码进行review，如果发现大量拷贝别人的代码，将扣分


