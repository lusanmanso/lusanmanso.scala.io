- HDFS:

bigdata> cd /home/bigdata/hadoop-3.3.6/
bigdata> sbin/start-dfs.sh
bigdata> sbin/stop-dfs.sh
navegador: http://localhost:9870
bigdata> bin/hdfs dfs -help
bigdata> bin/hdfs dfs -ls /
bigdata> bin/hdfs dfs -mkdir /ebooks
bigdata> wget https://www.gutenberg.org/cache/epub/2000/pg2000.txt -P /home/bigdata/Descargas/
bigdata> bin/hdfs dfs -put /home/bigdata/Descargas/pg2000.txt /ebooks/
bigdata> bin/hdfs dfs -copyFromLocal /home/bigdata/hadoop-3.3.6/README.txt /ebooks/
bigdata> bin/hdfs dfs -get /ebooks/README.txt /home/bigdata/Descargas/
bigdata> bin/hdfs dfs -cat /ebooks/README.txt
bigdata> bin/hdfs dfs -rm /ebooks/README.txt
bigdata> bin/hdfs dfs -mv /ebooks/pg2000.txt /ebooks/quijote.txt
bigdata> bin/hdfs dfs -du -h /ebooks/
bigdata> bin/hdfs dfs -count /ebooks
bigdata> bin/hdfs dfs -find / -name quijote.txt
bigdata> bin/hdfs dfs -head /ebooks/quijote.txt
bigdata> bin/hdfs dfs -tail /ebooks/quijote.txt

EJERCICIO:

- Investiga y ejecuta el comando fsck sobre /ebooks/quijote.txt. Observa su resultado.
	>>> SOLUCIÓN: bin/hdfs fsck /ebooks/quijote.txt
	>>> SOLUCIÓN: bin/hdfs fsck /ebooks/quijote.txt -files -blocks -locations
- Investiga y ejecuta el comando stat sobre /ebooks/quijote.txt. Observa su resultado. ¿Cómo mostrarías los permisos del fichero?
	>>> SOLUCIÓN: bin/hdfs dfs -stat /ebooks/quijote.txt
	>>> SOLUCIÓN: bin/hdfs dfs -stat %A /ebooks/quijote.txt
- Investiga y ejecuta el comando report. Explica su resultado.
	>>> SOLUCIÓN: bin/hdfs dfsadmin -report
