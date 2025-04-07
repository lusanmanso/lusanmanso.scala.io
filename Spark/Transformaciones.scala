Map Reduce con RDDs
    1. Preparación
        bigdata> wget http://www.gutenberg.org/cache/epub/2000/pg2000.txt
        spark> val quijoteRDD = sc.textFile("file:///home/bigdata/Descargas/pg2000.txt")
        spark> quijoteRDD.getNumPartitions
        spark> val quijoteRDD = sc.textFile("hdfs://localhost:9000/ebooks/quijote.txt", 10)
        spark> quijoteRDD.getNumPartitions
    2. ¿En cuántas líneas aparece la palabra "molino"?
        spark>  val quijoteRDD.map(_.toLowerCase).filter(l => l.contains("molino")).count
    3. ¿Cuántas palabras distintas aparecen en el texto?
        spark>  val quijoteRDD.flatMap(_.split(" ")).distinct.count
    4. ¿Cuales son las 10 palabras, de más de 3 letras, más repetida y cuántas veces?
        spark>  val quijoteRDD.flatMap(_.split(" ")).map(_.replaceAll("-","").replace(",","")).filter(_.length>3).map((_, 1)).reduceByKey(_ + _).map(p => (p._2, p._1)).sortByKey(false, 10).take(10).foreach(println)
    5. ¿Cuántas veces aparece la palabra “hidalgo”?
        spark>  val quijoteRDD.flatMap(_.split(" ")).map(_.toLowerCase).filter(_.contains("hidalgo")).count
    6. ¿Cuántas líneas tiene el texto?
        spark>  val quijoteRDD.count
    7. ¿Cuántas líneas tienen más de 20 caracteres?
        spark>  val quijoteRDD.filter(_.length > 20).count
    8. ¿Cuál es la línea más larga y con cuántos caracteres?
        spark>  val quijoteRDD.map(l => (l.length, l)).sortByKey(false).take(1)
