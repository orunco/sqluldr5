# sqluldr5 - Oracle Data Export Tool (Java Fast Version)

**Sqluldr5, A fast, memory-efficient tool for exporting Oracle data into PostgreSQL-compatible text format.**

---

In actual testing, using the same data source and identical SQL query:

|      | sqluldr2_bin | sqluldr5_java     |
| ---- | ------------ | ----------------- |
| Time | 4s           | 5s / 1.6GB memory |

## What is it?

Oracle provides a tool called SQL*Loader (`sqlldr`) for loading data from text files into the database. This tool, on the other hand, exports data from Oracle into text format — hence the name **SQL*UnLoader (`sqluldr`)**.

Around 2006, a DBA discovered that although he had implemented a data export tool using JDBC — which was relatively lightweight and portable due to Java's widespread runtime availability — it consumed up to four times more CPU than an OCI-based program when achieving similar performance. As a result, the Java version often caused the client machine’s CPU to max out, making it unsuitable for exporting large volumes of data (in the GB range). To address this, he developed the first version in C using the Oracle OCI 7 interface. `sqluldr2` was later rewritten using OCI 8.

The tool is known for being simple, **fast**, and feature-rich, and has gained some adoption among DBAs.

While `sqluldr2` is indeed fast, its source code is written in C, which makes it difficult to modify. Therefore, I decided to try reimplementing it using Java and JDBC to see if I could match its export speed. Although such an implementation is straightforward and can be done by any intern or AI, achieving comparable performance requires some special optimizations:

- Must use `ojdbc8-12.jar` or higher; Oracle likely optimized these drivers extensively for performance.
- Use `RandomAccessFile` for fast writing to target files — a fairly intuitive optimization.
- Must use Netty’s `ByteBuf` for byte-level writing.
- Cache field method information.
- Apply certain Oracle-side query optimization parameters.

## Getting Started

```bash
git clone https://github.com/orunco/sqluldr5.git

/jdk-17.0.2/bin/javac -classpath ./ojdbc8-12.2.0.1.jar:./netty-all-4.1.63.Final.jar:./ sqluldr5.java

/jdk-17.0.2/bin/java -classpath ./ojdbc8-12.2.0.1.jar:./netty-all-4.1.63.Final.jar:./ sqluldr5 oratest/oratest jdbc:oracle:thin:@127.0.0.1:1521:orcl "select * from t1"

```

Of course, the real value lies in referencing the code and adapting it to your own needs.

## License

Copyright (C) 2025 Pete Zhang, rivxer@gmail.com

Licensed under the Apache License, Version 2.0 (the "License");
