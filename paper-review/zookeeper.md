# ZooKeeper 简析

ZooKeeper 是一个无等待的分布式协调器，底层使用 ZAB 共识算法实现了容错机制。ZAB 实现原理与 raft 基本一致，所以在熟悉 raft 的基础上，理解其原理会很容易很多。

### 1. 数据模型
ZooKeeper 的数据模型是一个层次化的文件系统结构，其中数据被组织成一棵树形结构，每个节点被称为一个 znode。这种结构类似于 UNIX 文件系统目录树，同时也具有一定的特殊性。   

**znode 特点**   
1、znode 可以像目录一样充当路径标识，同时也可以像文件一样，保存少量数据，不区分目录与文件；   
2、znode 支持简单的读写操作，但不是存储常规的数据，而是通过保存一些元数据，用于协调多个进程间工作。
3、除了临时节点外，znode 都可以拥有子节点。  

**znode 类型**   
常规节点：创建后需要显示删除，否则会一直存在。  
临时节点：这类节点与创建它们的客户端会话绑定。一旦客户端会话结束（正常终止或者错误断开），这类节点就会自动被删除。当然，一样可以显示删除。   
序列节点：无论是持久节点还是临时节点，都可以加上序列标志。这意味着每当创建一个这样的节点时，ZooKeeper 都会在节点名后面追加一个唯一的递增数字，如 /parent/lock-{n}，n 为递增数字。这个特点可以用于实现锁原语。  

### 2. 一致性保证

#### 2.1 两个顺序保证：  
**Linearizable writes**       
所有写操作是序列化的，所有节点按照相同顺序执行写操作，节点会得到相同的状态。

**FIFO client order**   
每个客户端的读写操作会按照发送顺序执行。当客户端发送两个异步请求，第二个请求会看到第一个请求的结果。

#### 2.2 读写操作 
**写操作**  
必须经由主节点处理，主节点按顺序处理到达的写操作。

**读操作**   
主从节点都可以处理读操作；  
每个客户端都可以看见自己最后一次写操作的结果；  
一个客户端可能无法及时读取到其他客户端的更新，会读取到过时数据（stale data）；  
客户端不会从过去读取数据（read from the past）。   

问：如何理解客户端不会从过去读取数据？   
客户端从某节点读取数据，假设该节点的日志已经应用到 index X，可以表示客户端读取到的最新日志前缀为 X，在下一次读取数据时，客户端读取到的日志前缀必须大于 X。 

**弱一致性**   
显然，ZooKeeper 保证的是弱一致性，从每个单独的客户端角度看，它具有线性一致性；而从全局角度看，不能保证线性一致性，一个客户端能否立即看到另一个客户端的写入结果，是不确定的，它可能仍会读取到旧的数据。

### 3. 实现原理

#### 3.1 底层组件
Request Processor

Atomic Broadcast

#### 3.2 交互实现
使用 zxid 表示数据的更新程度，可以将其视日志条目对应的 index。   

每个客户端都会有一个对应的 zxid，每次读请求都会带上这个 zxid；   
客户端发送写请求，服务器会将写操作追加到日志，并应用条目，然后更新 zxid 响应给客户端；   
客户端下一次读请求，会带上最新的 zxid，如果处理读请求的节点还未应用到 zxid 相对应的日志条目，就会阻塞直到该条目被应用为止。


### 3. 原语示例

**Simple Locks**    
实现一个简单的互斥锁
```
 Lock
 1 n = create(l + “/lock-”, EPHEMERAL|SEQUENTIAL)
 2 C = getChildren(l, false)
 3 if n is lowest znode in C, exit
 4 p = znode in C ordered just before n
 5 if exists(p, true) wait for watch event
 6 goto 2

 Unlock
 1 delete(n)
```
**实现逻辑**： 设节点路径为 /Simple-Lock，每个客户端都会在 /Simple-Lock 下创建前缀为 /lock- 的序列节点，依次为 /lock-00001、/lock-00002、/lock-00003 ... ，如果节点的序列编号最小，就可以获取锁，否则，就需要等待前一个节点被删除，才能获取锁。   

**设计优点**：   
1、通过序列化的方式实现互斥锁，不会出现锁的争用，每次锁的释放只会唤醒一个客户端；    
2、通过事件通知的方式，不需要在服务端进行轮询操作，节省资源使用；   
3、将节点设置为临时节点，在客户端奔溃时，依然可以顺利释放锁，防止系统停滞。  

**Read/Write Locks**
```
 1 n = create(l + “/write-”, EPHEMERAL|SEQUENTIAL)
 2 C = getChildren(l, false)
 3 if n is lowest znode in C, exit
 4 p = znode in C ordered just before n
 5 if exists(p, true) wait for event
 6 goto 2
 Read Lock
 1 n = create(l + “/read-”, EPHEMERAL|SEQUENTIAL)
 2 C = getChildren(l, false)
 3 if no write znodes lower than n in C, exit
 4 p = write znode in C ordered just before n
 5 if exists(p, true) wait for event
 6 goto 3

```


**Configuration Management**


论文链接：
