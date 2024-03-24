# Method and Architecture to download an put an heavy element into a database(example: SUNAT-padron-rucs)

Please configure the application.properties in the root directory
using this exact parameters, this example contains the padron_reducido.zip
de la SUNAT(Peru) with 16 million parameters approximately which is free btw ;).

```application.properties
fileURL=https://www2.sunat.gob.pe/padron_reducido_ruc.zip
fileName=padron_reducido_ruc.zip
finalPathString=decompressed/padron_reducido_ruc.txt
DEFAULT_BATCH_SIZE=5000
LINK_URL=jdbc:sqlserver://10.10.10.60:1433;databaseName=SISDOCU
databaseUser=user
passwordUser=password
trustServerCertificate=true
rewriteBatchedStatements=true
threadBulk=20
```

## Details
- threadBulk: the amount of threads to use, it might depend on your pc resources. 
- DEFAULT_BATCH_SIZE: it represents the amount of registers to send into a 
database in this case is SQL Server, but easily could be MySql. 


