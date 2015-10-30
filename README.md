# kenshin

2015.8.19
lucene实现的搜索引擎demo

2015.8.27
Q：为什么必须先close writer，仅仅只commit的话，否则会报FileNotFund
A：Lucene对于commit有如下描述。源码中并没有判断。Note that this does not wait for any
running background merges to finish. This may be a costly operation, so
you should test the cost in your application and do it only when really
necessary.

2015.10.30
结构上做了较大改造，可以参考doc下的文档
