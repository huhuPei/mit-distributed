# Raft 简析

Raft 是用于分布式系统日志管理的共识算法，服务于上层状态机应用，通过保证分布式系统的状态一致性，从而对外形成高度可用的的单一状态机。

#### 1. 基本特点：
**1）三种状态：leader（领导者）、follower（追随者）、candidate（候选者）**   
领导者：每个任期只有一个领导者，通过选举选出，负责处理所有客户端的请求，同时向追随者追加日志条目。
追随者：被动接受来自请求领导者和候选者的请求，并做出响应。
候选者：启动新的任期，发起选举投票，选出新的领导者。    

系统正常运行时只有领导者和追随者，候选者只有在发起选举才会出现。

**2）Term（任期）**
任期机制可以说是 Raft 协议的核心，日志复制、领导选举、单一领导者都是围绕任期展开的。

**单一领导**
每一轮新的选举都会产生新的任期，新的任期意味着旧任期领导者失效，将让位于新的领导者。这是保持只有一个领导者的必要条件。

**信息过滤**
充当逻辑时钟，用于检测过时信息。当新任期开始时，节点会拒绝接受任何旧任期的请求，隔绝了旧任期的干扰。

**任期交换**
每个节点都会记录当前任期，在节点通信时，会交换这个任期信息，如果节点发现自己的任期更小，会进行更新，如果更大，会拒绝请求。这样可以让每个节点都顺利过渡到新任期。

**标记日志**
给每个日志条目添加任期信息，表示日志的新旧程度，在新任期产生领导者时，提供一致性检查，判断日志复制的起点。

**选举资格**
由于在某个时间节点上，各节点的状态更新程度不一致，存在一些慢节点，这些慢节点的日志并不是最新的，为了保证数据的完整性，只有日志比过半数节点更新（与任期相关的 up-to-date 原则），才有选举的资格。

**3）Strong leader（强领导者）**
Raft 的数据流向是单向的，日志条目只会从领导者流向其他节点。  
所有的客户端请求都必须经由领导者处理。

**4）single-round**
系统正常运行下，只需一轮通信，就可以对客户端操作达成一致。

#### 2. Leader election（领导选举）
**2.1 选举规则：**
**1）majority vote（多数投票）**   
发起选举投票：



**split brain（脑裂）**

**2）randomized election timeouts（随机选举超时）**   

**挑战：**
**split vote（分裂投票）**


#### 3. Log replication（日志复制）

**日志作用**
1）指令重传

2）执行顺序

3）持久化

4）缓存未提交操作


Log entry 提交时机
流程：append -> committed -> apply

log record = log index + command + term(leaders)

**日志一致性**
不同节点的日志包含相同顺序的相同命令。
这种性质使得各个节点的状态机会按照相同的顺序执行一系列相同的命令，得到相同的状态和相同的输出返回给客户端，最终，节点集群会表现为一个单一、高度可靠的状态机。

如何保证日志一致性？

一致性检查

#### 3. Safty（安全性）
**数据完整性**

如何保证领导者包含所有已提交的日志条目？
添加选举限制，限制哪些服务器可以成为领导者。
限制规则如下：候选者发送RequestVoteRPC时，包含自己的log信息，当追随者收到RPC后，
	        通过index和term比较两者间哪个logs更新，如果追随者更新，则拒绝投票，候选者将无法成为候选人。
up-to-date原则


4.其他
节点奔溃

客户端交互



新任期的领导者如何确定之前任期的日志条目是否已提交?

#### Membership changes

论文链接：