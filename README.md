# sqluldr5 - Oracle数据导出工具(java快速版)

**一个将Oracle数据导出成pg文本格式的、快速的、内存可控的工具。**

---

实际验证，相同的数据源，相同的sql查询语句：

|      | sqluldr2_bin | sqluldr5_java  |
| ---- | ------------ | -------------- |
| 时间 | 4s           | 5s / 1.6GB内存 |



## What

Oracle 有一个工具叫 SQL*Loader(sqlldr)是用来将文本文件装载到数据库中的 ，本工具是用来将 Oracle 中的数据导出成文本的，因此取名为 SQL*UnLoader(sqluldr)。

大概在2006年，有一个DBA楼方鑫，因为发现"**用 JDBC 实现了一个数据导出工具，算是比较轻量级的了，基本上 Java 的运行环境哪儿都有，在测试速度时，发现与 OCI 程序比要达到同样的速度，运行 Java 程序的机器上多耗了四倍的 CPU，也就是用 Java 版本的文本导出程序常常让客户端的 CPU 跑满，因此无法面对更大量（上 GB）的数据导出**"，所以用Oracle OCI 7接口以及C语言写了第一版，sqluldr2是用OCI 8重写的第二版。工具的特点就是：简洁、**快速**、功能丰富。在DBA的圈子里面有一些用户。

虽然确实快速，但是sqluldr2源代码是c，很难修改，于是我决定用java jdbc接口再试试，看看能不能追平sqluldr2的导出速度？当然这种代码任何一个实习生/AI都很容易写出来，然而平凡无奇的实现一般确实要慢很多，需要经过一些特殊的实现技巧，才能大致追平，分别是：

- 必须使用ojdbc8-12.jar及以上的jdbc驱动，oracle应该是做了极致的性能优化
- 使用RandomAccessFile实现快速写目标文件，这个应该很容易想到
- 必须使用netty的ByteBuf，逐字节写入
- 缓存字段method信息
- 源端oracle的一些查询优化参数



## Getting Started
``` bash
git clone https://github.com/orunco/sqluldr5.git

/jdk-17.0.2/bin/javac -classpath ./ojdbc8-12.2.0.1.jar:./netty-all-4.1.63.Final.jar:./ sqluldr5.java

/jdk-17.0.2/bin/java -classpath ./ojdbc8-12.2.0.1.jar:./netty-all-4.1.63.Final.jar:./ sqluldr5 oratest/oratest 
jdbc:oracle:thin:@127.0.0.1:1521:orcl "select * from t1"
run test.html
```
当然，更加有意义的是参考代码，直接改成你需要的样子。



## License

Copyright (C) 2025 Pete Zhang, rivxer@gmail.com

Licensed under the Apache License, Version 2.0 (the "License");