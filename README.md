

Jungle
======
Embedded key-value storage library, based on a combined index of [LSM-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) and [copy-on-write B+tree](https://www.usenix.org/legacy/events/lsf07/tech/rodeh.pdf). Please refer to our [paper](https://www.usenix.org/conference/hotstorage19/presentation/ahn).

Jungle is specialized for building [replicated state machine](https://en.wikipedia.org/wiki/State_machine_replication) of consensus protocols such as [Paxos](https://en.wikipedia.org/wiki/Paxos_(computer_science)) or [Raft](https://raft.github.io/), by providing chronological ordering and lightweight persistent snapshot.


Features
--------
* Ordered mapping of key and its value on disk (file system). Both key and value are arbitrary length binary.
* Monotonically increasing sequence number for each key-value modification.
* Point lookup on both key and sequence number.
* Range lookup on both key and sequence number, by using iterator:
    * Snapshot isolation: each individual iterator is a snapshot.
    * Bi-directional traversal and jump: `prev`, `next`, `gotoBegin`, `gotoEnd`, and `seek`.
* Lightweight persistent snapshot, based on sequence number:
    * Nearly no overhead for the creation of a snapshot.
    * Snapshots are durable; preserved even after process restart.
* Tunable configurations:
    * The number of threads for log flushing and compaction.
    * Custom size ratio between LSM levels.
    * Compaction factor (please refer to the paper).
* Log store mode:
    * Ordered mapping of sequence number and value, eliminating key indexing.
    * Lightweight log truncation based on sequence number.

### Things we DO NOT (and also WILL NOT) support
* Secondary indexing, or SQL-like query:
    * Jungle will not understand the contents of value. Value is just a binary from Jungle's point of view.
* Server-client style service, or all other network-involving tasks such as replication:
    * Jungle is a library that should be embedded into your process.


Benefits
--------
Compared to other widely used LSM-based key-value storage libraries, benefits of Jungle are as follows:

* Smaller write amplification.
    * Jungle will have 4-5 times less write amplification, while providing the similar level of write performance.
* Chronological ordering of key-value pairs
    * Along with persistent logical snapshot, this feature is very useful when you use it as a replicated state machine for Paxos or Raft.



How to Build
------------
#### 1. Install `cmake`: ####
* Ubuntu
```sh
$ sudo apt-get install cmake
```

* OSX
```sh
$ brew install cmake
```

#### 2. Build ####
```sh
jungle$ ./prepare.sh -j8
jungle$ mkdir build
jungle$ cd build
jungle/build$ cmake ../
jungle/build$ make
```

Run unit tests:
```
jungle/build$ ./runtests.sh
```


How to Use
----------
Please refer to [this document](./docs/how_to_use.md).


Example Implementation
-----------------------
Please refer to [examples](./examples).


Supported Platforms
-------------------
* Ubuntu (tested on 14.04, 16.04, and 18.04)
* Centos (tested on 7)
* OSX (tested on 10.13 and 10.14)

#### Platforms will be supported in the future
* Windows


Contributing to This Project
----------------------------
We welcome contributions. If you find any bugs, potential flaws and edge cases, improvements, new feature suggestions or discussions, please submit issues or pull requests.


Contact
-------
* Jung-Sang Ahn <junahn@ebay.com>


Coding Convention
-----------------
* Recommended not to exceed 90 characters per line.
* Indent: 4 spaces, K&R (1TBS).
* Class & struct name: `UpperCamelCase`.
* Member function and member variable name: `lowerCamelCase`.
* Local variable, helper function, and parameter name: `snake_case`.

```C++
class MyClass {
public:
    void myFunction(int my_parameter) {
        int local_var = my_parameter + 1;
        if (local_var < myVariable) {
            // ...
        } else {
            // ...
        }
    }
private:
    int myVariable;
};

int helper_function() {
    return 0;
}
```

* Header include order: local to global.
    1. Header file corresponding to this source file (if applicable).
    2. Header files in the same project (i.e., Jungle).
    3. Header files from the other projects.
    4. C++ system header files.
    5. C system header files.
    * Note: alphabetical order within the same category.
    * Example (`my_file.cc`):
```C++
#include "my_file.h"            // Corresponding header file.

#include "table_file.h"         // Header files in the same project.
#include "table_helper.h"

#include "forestdb.h"           // Header files from the other projects.

#include <cassert>              // C++ header files.
#include <iostream>
#include <vector>

#include <sys/stat.h>           // C header files.
#include <sys/types.h>
#include <unistd.h>
```

License Information
--------------------
Copyright 2017-2019 eBay Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.


3rd Party Code
--------------
1. URL: https://github.com/couchbase/forestdb<br>
License: https://github.com/couchbase/forestdb/blob/master/LICENSE<br>
Originally licensed under the Apache 2.0 license.

2. URL: https://github.com/stbrumme/crc32<br>
Original Copyright 2011-2016 Stephan Brumme<br>
See Original ZLib License: https://github.com/stbrumme/crc32/blob/master/LICENSE

3. URL: https://github.com/greensky00/simple_logger<br>
License: https://github.com/greensky00/simple_logger/blob/master/LICENSE<br>
Originally licensed under the MIT license.

4. URL: https://github.com/greensky00/testsuite<br>
License: https://github.com/greensky00/testsuite/blob/master/LICENSE<br>
Originally licensed under the MIT license.

5. URL: https://github.com/greensky00/latency-collector<br>
License: https://github.com/greensky00/latency-collector/blob/master/LICENSE<br>
Originally licensed under the MIT license.

6. URL: https://github.com/eriwen/lcov-to-cobertura-xml/blob/master/lcov_cobertura/lcov_cobertura.py<br>
License: https://github.com/eriwen/lcov-to-cobertura-xml/blob/master/LICENSE<br>
Copyright 2011-2012 Eric Wendelin<br>
Originally licensed under the Apache 2.0 license.

7. URL: https://github.com/bilke/cmake-modules<br>
License: https://github.com/bilke/cmake-modules/blob/master/LICENSE_1_0.txt<br>
Copyright 2012-2017 Lars Bilke<br>
Originally licensed under the BSD license.

8. URL: https://github.com/aappleby/smhasher/tree/master/src<br>
Copyright 2016 Austin Appleby<br>
Originally licensed under the MIT license.
