Basic Operations
===

All APIs and their parameters are explained in [header files](../include/libjungle):
* [db_config.h](../include/libjungle/db_config.h): DB configurations.
* [db_stats.h](../include/libjungle/db_stats.h): DB statistics.
* [iterator.h](../include/libjungle/iterator.h): Iterator operations.
* [jungle.h](../include/libjungle/jungle.h): DB operations.
* [keyvalue.h](../include/libjungle/keyvalue.h): Key-value pair.
* [record.h](../include/libjungle/record.h): Record.
* [sized_buf.h](../include/libjungle/sized_buf.h): Basic buffer.
* [status.h](../include/libjungle/status.h): Operation result status.

The basic unit of indexing in Jungle is a [record](../include/libjungle/record.h), which consists of [key-value pair](../include/libjungle/keyvalue.h), sequence number, operation type, and custom metadata. Key, value, and metadata are based on variable size buffer (i.e., a memory buffer and its length), called [SizedBuf](../include/libjungle/sized_buf.h) in Jungle.


### Contents
* [Initialization](#initialization)
* [Open and close](#open-and-close)
* [Set operation](#set-operation)
* [Get operation](#get-operation)
* [Delete operation](#delete-operation)
* [Iterator](#iterator)

-----
### [Initialization](#contents)

Jungle defines various global resources shared among different DB instances in the same process, such as cache and thread pool. You can explicitly initialize or release them.

Initialize:
```C++
GlobalConfig global_config;
// ... set your config ...
jungle::init(global_config);
```

Release:
```C++
jungle::shutdown();
```

Note that above APIs can be skipped. In such case, initialization will be done on the first DB open with the default configurations, and they will be release at the termination of the process.


-----
### [Open and close](#contents)

Each Jungle instance is a single directory which contains multiple files such as DB logs, tables, and some debugging logs. To open a DB instance, a path to the DB should be given.
```C++
DBConfig db_config;
// ... set your config ...
DB* db = nullptr;
Status s = DB::open(&db, "./my_db", db_config);
```
Once opening DB is successful, `s.ok()` will be `true`. Otherwise, you can see the result value by calling `s.getValue()` or `s.toString()`.

If the given path does not exist or empty, Jungle will create a new one.

You can close the DB instance by calling `close` API:
```C++
Status s = db->close();
```


-----
### [Set operation](#contents)

There are four different ways to set (upsert) a record.

##### Set a key value pair: `jungle::set`.
* Custom metadata will be empty, and sequence number will be automatically generated.
```C++
db->set(KV("key", "value"));
```

##### Set a key value pair with custom sequence number: `jungle::setSN`.
  * Custom metadata will be empty, and sequence number will be set to the given number.
  * Sequence number should be unique and increasing. If not, undefined behavior including system crash or data corruption will happen.
```C++
KV key_value;
db->setSN(100, KV("key", "value"));
```


##### Set a record: `jungle::setRecordByKey`.
  * Custom metadata will be set to the given data, and sequence number will be automatically generated.
```C++
Record record;
// ... set record ...
db->setRecordByKey(record);
```


##### Set a record with custom sequence number: `jungle::setRecord`.
  * Both custom metadata and sequence number will be set to the given values.
  * Sequence number should be unique and increasing. If not, undefined behavior including system crash or data corruption will happen.
```C++
Record record;
// ... set record ...
record.seqNum = 100;
db->setRecord(record);
```


-----
### [Get operation](#contents)

There are four different ways to get (point query) a record. User is responsible for the deallocation of memory returned by get operations.

##### Get a value corresponding to given key: `jungle::get`.
* Only value part will be returned.
* `get` on deleted key will not succeed.
```C++
SizedBuf returned_value;
db->get(SizedBuf("key_to_find", returned_value);
returned_value.free();
```

##### Get a key value pair corresponding to given sequence number: `jungle::getSN`.
* Key and value will be returned.
* `getSN` on deleted key will not succeed.
* Note: *currently we support this API only for log store mode.*
```C++
KV returned_key_value;
db->getSN(100, returned_key_value);
returned_key_value.free();
```


##### Get a record corresponding to given key: `jungle::getRecordByKey`.
* All fields in record will be returned.
* There is a flag to retrieve a deleted record.
* Below example will not succeed on deleted record:
```C++
Record returned_record;
db->getRecordByKey(SizedBuf("key_to_find", returned_record);
returned_record.free();
```

* With setting the flag to `true`, deleted record will be returned. Custom metadata will be retrieved if it was set before when the record was deleted. Value part of the returned record will be empty.
* Only logically deleted records will be visible. Once a record is physically deleted by merge or compaction, it will not be retrieved anymore.
```C++
Record returned_record;
db->getRecordByKey(SizedBuf("key_to_find", returned_record, true);
returned_record.free();
```


##### Get a record corresponding to given sequence number: `jungle::getRecord`.
* All fields in record will be returned.
* `getRecord` on deleted key will always succeed.
* Note: *currently we support this API only for log store mode.*
```C++
Record returned_record;
db->getRecord(100, returned_record);
returned_record.free();
```

-----

### [Delete operation](#contents)

In Jungle, delete operation is the same as update operation, modifying the existing record as a deletion marker (i.e., tombstone). You can put your custom metadata for each tombstone and retrieve it later. Tombstones are physically purged later, during Table compaction.

There are three different ways to delete a record.

##### Delete a key and its value: `jungle::del`.
* Sequence number for this tombstone will be automatically generated.
```C++
db->del(SizedBuf("key_to_delete");
```

##### Delete a key and its value with custom sequence number: `jungle::delSN`.
  * Sequence number for this tombstone will be set to the given number.
  * Sequence number should be unique and increasing. If not, undefined behavior including system crash or data corruption will happen.
```C++
db->delSN(100, SizedBuf("key_to_delete");
```

##### Delete a record with custom metadata and sequence number: `jungle::setRecord`.
  * Both custom metadata and sequence number for this tombstone will be set to the given values.
  * Sequence number should be unique and increasing. If not, undefined behavior including system crash or data corruption will happen.
```C++
Record record;
record.meta = ... // metadata for this tombstone
record.seqNum = 100;
db->setRecord(record);
```

-----
### [Iterator](#contents)

Each iterator works as a snapshot, thus any mutations will not be applied to previous iterators already opened.

Jungle can create an iterator in two different orders: key and sequence number.


#### Opening an iterator from DB instance

##### Key iterator

```C++
Iterator itr;
itr.init(db);
```
This iterator can access all keys in the given `db`. You can also specify the range.
```C++
itr.init(db, SizedBuf("a"), SizedBuf("z")); // from a to z (inclusive)
```
or
```C++
itr.init(db, SizedBuf("a")); // from a to max
```
or
```C++
itr.init(db, SizedBuf(), SizedBuf("z")); // from min to z
```

##### Sequence number iterator

```C++
Iterator itr;
itr.initSN(db);
```
This iterator can access all sequence numbers in the given `db`. You can also specify the range.
```C++
itr.initSN(db, 100, 200); // from 100 to 200 (inclusive)
```
or
```C++
itr.initSN(db, 100); // from 100 to max
```
or
```C++
itr.initSN(db, DB::NULL_SEQNUM, 200); // from min to 200
```

##### Note

Even though the DB instance is empty or there is no record within the given range, opening an iterator will succeed. But any following operations on that iterator will return error.

#### Closing an iterator

Both key and sequence number iterators can be closed using `DB::Iterator::close` API.
```C++
itr.close();
```
All iterators should be closed before the closing the parent DB instance.


#### Get a record at the current cursor

```C++
Record returned_record;
Status s = itr.get(returned_record);
returned_record.free();
```
If the iterator currently does not point to any record, `s.ok()` will be `false`.

Same as `get` operations, user is responsible for the deallocation of the returned record.

Sequence number iterator will return tombstones, while key iterator will return live records only.

#### Move the cursor of the iterator

##### Forward
```C++
Status s = itr.next();
```
If the cursor is successfully moved, `s.ok()` will be `true`.

##### Backward
```C++
Status s = itr.prev();
```
If the cursor is successfully moved, `s.ok()` will be `true`.

##### Jump to begin
```C++
Status s = itr.gotoBegin();
```
If the cursor is successfully moved, `s.ok()` will be `true`.

##### Jump to end
```C++
Status s = itr.gotoEnd();
```
If the cursor is successfully moved, `s.ok()` will be `true`.

##### Jump to a random position (key iterator)
```C++
Status s = itr.seek(SizedBuf("key_to_find");
```
If the given key does not exist, this API will find the smallest but greater then the given key. If such key does not exist either, `s.ok()` will be `false`.

There is an option to choose the behavior when the exact match does not exist. If you explicitly set it as follows:
```C++
Status s = itr.seek(SizedBuf("key_to_find", Iterator::SMALLER);
```
then it will find the greatest but smaller than the given key.

##### Jump to a random position (sequence number iterator)
```C++
Status s = itr.seekSN(100);
```
Other things are identical to those of key iterator.


