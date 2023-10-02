
## Lab1

[Go语言](https://www.youtube.com/watch?v=IdCbMO0Ey9I)
思路 
https://blog.csdn.net/weixin_45938441/article/details/124018485
https://zhuanlan.zhihu.com/p/260752052
https://ray-eldath.me/programming/deep-dive-in-6824/

> 比我想象的难一些(我太菜辣)，csdiy上提供的链接只讲了实现难点，看不太懂，因为我目前还在进行基本思路的思考，主要是仔细看官网上的讲义、Lab介绍和大佬写的课程中文翻译，我的英文阅读能力还是有待增强，论文不太读得下去

### 任务: 实现 coordinator  worker rpc

Our job is to implement a distributed MapReduce, consisting of two programs, the coordinator and the worker. There will be just one coordinator process, and one or more worker processes executing in parallel. In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine. 

The workers will talk to the coordinator via *RPC*. Each worker process will ask the coordinator for a task, read the task's input from one or more files, execute the task, and write the task's output to one or more files. **The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.**

> We have given you a little code to start you off. The "main" routines for the coordinator and worker are in `main/mrcoordinator.go` and `main/mrworker.go`; don't change these files. You should put your implementation in `mr/coordinator.go`, `mr/worker.go`, and `mr/rpc.go`.



MapReduce hides many details:
  sending app code to servers
  tracking which tasks have finished
  "shuffling" intermediate data from Maps to Reduces
  balancing load over servers
  recovering from failures

  The "Coordinator" manages all the steps in a job.
  1. coordinator gives Map tasks to workers until all Maps complete
     Maps write output (intermediate data) to local disk
     Maps split output, by hash(key) mod R, into one file per Reduce task
  2. after all Maps have finished, coordinator hands out Reduce tasks
     each Reduce task corresponds to one hash bucket of intermediate output
     each Reduce fetches its intermediate output from (all) Map workers
     each Reduce task writes a separate output file on GFS

### 本人思考

1. worker 

通过 RPC 获取 task，并执行

Map 得到很多 k-v 对(其中有大量重复的k)  emit 传入k为单词，v为'1'

Reduce 由 k-v 对 得到每一个k 所对应的k-v对的个数  emit 传入v为每个k对应的个数(数组的长度)

很多 Map 可以并行，每个读取一个文件，得到一个 intermediate data  存入对应桶中，文件名 mr-X-Y 

全部运行完之后则运行Reduce，Reduce应该读对应哈希桶中的数据(从所有mr-*-Y文件中读取键值对)，输出到对应桶中

一开始的想法是使用 filepath.Walk 遍历全部文件再判断后缀是否匹配，感觉不够优雅；后来选择了直接通过参数传入文件名 

2. coordinator 

分配任务 首先分配Map任务，并等待全部Map任务执行完成，之后分配Reduce任务，并等待Reduce任务执行完成
最开始得到文件名 files，收到分配任务的请求时，分配一个Map任务，
需要检测是否完成任务，逐个检查麻烦，让worker在完成任务时发送消息
注意，被分配的任务10s没完成则会被分配给其他worker，需要特别检查

RPC 构成: 任务类型 索引(任务编号) 
Map的文件名 Map任务数
Reduce任务数

strconv.Itoa 整数转换为字符串

用锁来实现并发

没加锁的单线程实现，遇到错误 循环中没有对变量重新初始化，而在通过RPC传递时，有些没用到的变量又没有更改，具体如下

+ Go RPC sends only struct fields whose names start with capital letters. Sub-structures must also have capitalized field names.
+ When calling the RPC call() function, the reply struct should contain all default values. RPC calls should look like this:
  reply := SomeType{}
  call(..., &reply)
without setting any fields of reply before the call. If you pass reply structures that have non-default fields, the RPC system may silently return incorrect values.

简陋的实现了一版，测试中除了crash test，别的都过了，但还没实现并行和加锁

我的实现中 coordinator 用计数来检测任务是否完成，虽然简单且高效，但有很多弊端，完全不清楚具体的任务完成和未完成的情况，遇到 worker 崩溃时无法解决，鲁棒性差，并非好的实现。

better 实现:
coordinator用tasks数组来记录任务的完成情况，用锁来保证并发
worker获取任务并执行，更新任务状态，
coordinator在worker获取任务时，需对全部任务的状态进行检测来得到待分配的任务(为了避免每次遍历所有任务来获取，维护一个队列？)

需要的结构
Coordinator: files, nReduce, nMap, assignedTasks(全部任务应该由Coordinator来管理), remainingTasks, mutex
  两个方法，获取任务(Task->GetTaskResponse)，通知任务完成
任务状态: 待分配<->已分配->已完成 
  待分配的任务用队列来存，已分配的任务用Map来存(方便查找修改)，任务需要有一个唯一的编号，用于标识任务和在Map中进行索引(任务编号从0开始，重新分配任务时，编号不变)
  阶段: Map, Reduce
  Map阶段中，如果待分配和已分配的任务都为空，则进入Reduce阶段

读写的原子性 先生成一个临时文件再利用系统调用 `OS.Rename` 来完成原子性替换，这样即可保证写文件的原子性。
处理崩溃
最长等待10秒 问题，如果待分配任务为空，而已分配任务未做完，有worker请求任务，此时应该等待 

离谱，我都不知道 coordinator 中哪里可以加入并行，直接`go func() {...}()`，这样太呆了，而且没有阻塞

+ The coordinator, as an RPC server, will be concurrent; don't forget to lock shared data.
+ The coordinator can't reliably distinguish between crashed workers, workers that are alive but have stalled for some reason, and workers that are executing but too slowly to be useful. **The best you can do is have the coordinator wait for some amount of time, and then give up and re-issue the task to a different worker.** For this lab, have the coordinator wait for ten seconds; after that the coordinator should assume the worker has died (of course, it might not have).
+ If you choose to implement Backup Tasks (Section 3.6), note that we test that your code doesn't schedule extraneous tasks when workers execute tasks without crashing. Backup tasks should only be scheduled after some relatively long period of time (e.g., 10s).
+ To test crash recovery, you can use the mrapps/crash.go application plugin. It randomly exits in the Map and Reduce functions.
+ To ensure that nobody observes partially written files in the presence of crashes, the MapReduce paper mentions the trick of using a temporary file and atomically renaming it once it is completely written. You can use ioutil.TempFile to create a temporary file and os.Rename to atomically rename it.

竞争检测器

Use Go's race detector, with go run -race. test-mr.sh has a comment at the start that tells you how to run it with -race. When we grade your labs, we will not use the race detector. Nevertheless, if your code has races, there's a good chance it will fail when we test it even without the race detector.

运行
```
go build -buildmode=plugin ../mrapps/wc.go
rm mr-*

go run mrcoordinator.go pg-*.txt

go run mrworker.go wc.so
```

## Lab2

