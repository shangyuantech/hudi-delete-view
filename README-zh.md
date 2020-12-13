# 删除处理

删除处理的逻辑：

* 构建删除视图对象，记录涉及删除的文件和对应上次提交的文件（目前只针对COW表）

* 根据涉及的删除文件构建rdd，只需要记录key就可以了

* 处理rdd，操作为加载数据的key，然后利用parquet的reader读取，如果读取的数据不在set里面则作为删除数据。
  注：如果之前已经做过删除查询操作，则可直接读取历史文件，并省略下一步的保存操作。
  
* 保存删除数据（如果是第一次查询）

* 查询删除数据为spark视图。具体查询方式可以类似如下：

```java
String hudiPath = "/hive/warehouse/test.db/test/";
String timstamp = "202012121212";

DeleteSupport deleteSupport = new DeleteSupport(hudiPath, timstamp);
Dataset<Row> deleteRows = deleteSupport.getDeleteDataset();
deleteRows.show();
```