Copyright (c) 2013 Cloudera, Inc. All rights reserved.

This directory contains the CSD for Record Service

## Build CSD and schema-validator jar
<pre>
$ cd $RECORD_SERVICE_HOME/csd
$ mvn clean install
</pre>

## Validate sdl
<pre>
$ cd $RECORD_SERVICE_HOME/csd/RECORD_SERVICE
$ java -jar target/schema-validator-5.4.0.jar -s src/descriptor/service.sdl
Validating: src/descriptor/service.sdl
Validation succeeded.
</pre>

## Add a parcel
- go to cm-server:7180/cmf/parcel/status
- click on download / distribute / activate parcel
- restart Cloudera Management Service

## Add a service via csd
- ssh to cloudera manager server machine
- copy $RECORD_SERVICE_HOME/csd/RECORD_SERVICE/target/RECORD_SERVICE-1.0.jar to /opt/cloudera/csd
- sudo service cloudera-scm-server restart
- check added csd in cm-server:7180/cmf/csd/refresh
- add a service in cm-server:7180/cmf/home

## Tutorials
- http://github.mtv.cloudera.com/Starship/cm_ext/wiki
- http://github.mtv.cloudera.com/Starship/csd