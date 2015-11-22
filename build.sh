mvn -DskipTests package assembly:single
cd hbase-assembly/target
tar xvzf hbase-1.2.0-SNAPSHOT-bin.tar.gz
cp ../../hbase-site.xml hbase-1.2.0-SNAPSHOT/conf/
cd hbase-1.2.0-SNAPSHOT/bin
. start-hbase.sh
