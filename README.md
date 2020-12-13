# Delete processing

The logic of delete processing :

* Build a delete view java object to record the deleted files and the corresponding last submitted files. (COW is supported now)

* The RDD is constructed according to the deleted files involved, and only the keys needs to be recorded.

* To process RDD, the operation is to load the keys of the data, and then use the reader of parquet to read it. If the read data is not in the set, it will be marked as the deleted data.

    Note: if you have done the delete query operation before, you can read the history file directly and omit the next save operation.

* Save delete data (if it's the first time to query)

* Query delete data as a spark view. The specific query methods are as follows:

```java
String hudiPath = "/hive/warehouse/test.db/test/";
String timstamp = "202012121212";

DeleteSupport deleteSupport = new DeleteSupport(hudiPath, timstamp);
Dataset<Row> deleteRows = deleteSupport.getDeleteDataset();
deleteRows.show();
```