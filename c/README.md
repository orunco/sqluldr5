# Sqluldr

Sqluldr originated from tbuldr.

## Compile

http://www.ningoo.net/html/2009/learn_oci_programming_from_ociuldr.html

``` bash
export LD_LIBRARY_PATH=/u01/app/oracle/product/11.2.0/dbhome_1/lib/

gcc -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -I/u01/app/oracle/product/11.2.0/dbhome_1/rdbms/public -I/u01/app/oracle/product/11.2.0/dbhome_1/rdbms/demo -L/u01/app/oracle/product/11.2.0/dbhome_1/lib -lclntsh -o tbuldr tbuldr.c
```

## Run
``` bash
export LD_LIBRARY_PATH=/u01/app/oracle/product/11.2.0/dbhome_1/lib/

./tbuldr user=test/test@oratest query="select * from table where rownum<1"  field=0x01 file=test.txt
./tbuldr user=test/test@oratest query=table  field=0x01 record=0x02 file=test.txt

```

## Patch
When writing to a target in pg format text, special handling is required. Refer to `tbuldr.patch`






