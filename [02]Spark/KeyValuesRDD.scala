sval rdd = sc.parallelize(Seq((1, 2), (3, 4), (3, 6)))

// ReduceByKey
rdd.reduceByKey(_ + _, 4).collect()

// Ejercicio1: ¿Qué método utilizarías para saber el número de particiones por defecto para reduceByKey? Explica a qué corresponde ese número de particiones
rdd.reduceByKey(_ + _).getNumPartitions //Número de cores
// Ejercicio2: Utiliza el método reduceByKey para obtener el valor máximo de cada clave
// Resultado esperado: Array((1,2), (3,6))
rdd.reduceByKey{ case (v1, v2) => if(v1>v2) v1 else v2 }.collect()
rdd.reduceByKey(Math.max(_, _)).collect()

// CountByKey
rdd.countByKey()

// CountByValue
rdd.countByValue()

// GroupByKey
rdd.groupByKey()
rdd.groupByKey().collect()
rdd.groupByKey().map(x => (x._1, x._2.toList)).collect()

// Ejercicio3: Utiliza el método groupByKey + otra transformación para obtener el valor mínimo de cada clave
// Resultado esperado: Array((1,2), (3,4))
rdd.groupByKey().map(x => (x._1, x._2.min)).collect()

// MapValues
rdd.groupByKey().mapValues(_.toList).collect()
rdd.mapValues(_+1).collect()

// Ejercicio4: Utiliza el método mapValues para multiplicar por 3 solo los valores múltiplos de 3
// Resultado esperado: Array((1,2), (3,4), (3,18))
rdd.mapValues(x => if (x%3==0) x*3 else x).collect()

// CombineByKey
val salarios = sc.parallelize(Seq(("Ventas", 1500), ("Ventas", 2100), ("TI", 2200), ("TI", 2500), ("Marketing", 1600)))
    // La función para crear el acumulador inicial es simplemente el valor
val createAccumulator = (salary: Int) => (salary, 1)
    // La función para agregar un valor al acumulador existente es agregar el valor al total y aumentar el contador
val addValue = (accumulator: (Int, Int), salary: Int) => (accumulator._1 + salary, accumulator._2 + 1)
    // La función para fusionar dos acumuladores es sumar los totales y los contadores
val mergeAccumulators = (accumulator1: (Int, Int), accumulator2: (Int, Int)) => (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)
    // Aplicamos combineByKey con las funciones definidas anteriormente
val result = salarios.combineByKey(createAccumulator, addValue, mergeAccumulators)
result.collect()
    // Calculamos el salario promedio por departamento
val averageSalaryPerDept = result.mapValues { case (total, count) => total / count.toDouble }
    // Mostramos el resultado
averageSalaryPerDept.map{case (departamento, salario) => s"Salario medio en $departamento: $salario"}.collect().foreach(println)

// Ejercicio 5: Utiliza CombineByKey para calcular el salario más alto de cada departamento
// Resultado esperado: Salario máximo en TI: 2500\n Salario máximo en Marketing: 1600\n Salario máximo en Ventas: 2100
val createAccumulator = (salary: Int) => (salary)
val addValue = (accumulator: Int, salary: Int) => Math.max(accumulator, salary)
val mergeAccumulators = (accumulator1: Int, accumulator2: Int) => Math.max(accumulator1, accumulator2)
val result2 = salarios.combineByKey(createAccumulator, addValue, mergeAccumulators)
result2.map{case (departamento, salario) => s"Salario máximo en $departamento: $salario"}.collect().foreach(println)

// FlatMapValues
val rdd2 = sc.parallelize(Seq(("pares", Seq(2, 4, 6)), ("impares", Seq(1, 3))))
rdd2.collect()
rdd2.flatMapValues(x => x.map(_*2)).collect()

// Ejercicio 6: Utiliza flatMapValues para obtener el número máximo de cada grupo
// Resultado esperado: Array((pares,6), (impares,3))
rdd2.flatMapValues(x => Seq(x.max)).collect()

// Keys & Values
rdd.keys
rdd.values

// SortByKey
rdd.sortByKey().collect()

// Ejercicio 7: ¿Cómo ordenarías el resultado del ejercicio 5 por VALOR de mayor a menor (orden descendente)?
// Resultado esperado: Array((TI,2350), (Ventas,1800), (Marketing,1600))
averageSalaryPerDept.map(_.swap).sortByKey(false).map(_.swap).collect()

// SubtractByKey
val other = sc.parallelize(Seq((3, 9)))
rdd.subtractByKey(other).collect()

// Join
rdd.join(other).collect()
rdd.rightOuterJoin(other).collect()
rdd.leftOuterJoin(other).collect()

// Ejercicio 8: Partiendo de rdd.leftOuterJoin(other), ¿cual es el sumatario por clave?
// Resultado esperado: Array((1,2), (3,28))
rdd.leftOuterJoin(other).mapValues{ case(a, Some(b)) => a+b; case(a, None) => a }.reduceByKey(_ + _).collect()
rdd.leftOuterJoin(other).mapValues{ case(a, b) => a+b.getOrElse(0) }.reduceByKey(_ + _).collect()

// Other actions
rdd.collectAsMap()
rdd.lookup(3)

// Ejercicio 9: Filtrar claves que tengan asociados al menos 2 valores.
// Mantener las llaves que tienen por lo menos 2 repeticiones y borrar las que no se repiten
// Resultado esperado: Array((key1,1), (key1,2), (key1,4), (key2,1), (key2,3), (key4,1), (key4,1))
val my_rdd = sc.parallelize(Seq(("key1", 1), ("key2", 1), ("key1", 2), ("key2", 3), ("key4", 1), ("key1", 4), ("key4", 1), ("key6", 2), ("key7", 4), ("key8", 5), ("key9", 6), ("key10", 7)))
val filter_rdd = my_rdd.groupByKey().mapValues(_.toList).filter(x => x._2.length >= 2).flatMap(x => x._2.map((x._1, _)))
val filter_rdd = my_rdd.groupByKey().filter(x => x._2.size >= 2).flatMap(x => x._2.map((x._1, _)))
filter_rdd.collect().foreach(println)

// Ejercicio 10: Preguntas: ¿Qué alumnos que no están de Erasmus han aprobado ambas asignaturas?
// Tupla (nombre, Seq(asistencia, parcial, practica, examen))
// Calcular nota final: 10%asistencia + 20%parcial + 20%practica + 50%examen (examen>=4)
// Resultado esperado: Array((Manolito,5.1000000000000005))
val clase1 = sc.parallelize(Seq(("Juanito", Seq(10, 5, 9, 3)), ("Pepito", Seq(8, 8, 5, 8)), ("Manolito", Seq(7, 3, 9, 4))))
val alumnos_erasmus = sc.parallelize(Seq(("Pepito", Seq.empty[Int])))
clase1.subtractByKey(alumnos_erasmus).filter{ case (alumno, notas) => notas.last >= 4 }.mapValues(notas => 0.1*notas(0)+0.2*notas(1)+0.2*notas(2)+0.5*notas(3)).filter(_._2 >= 5).collect()

// Ejercicio 11: El resultado del ejercico 9 se va a guardar en HDFS
// Previamente se debe iniciar HDFS, ¿cómo lo harías?
ubuntu> cd /home/bigdata/hadoop-3.3.6/
ubuntu> sbin/start-dfs.sh
// Para guardar dicho rdd en la ruta /ejercicios/ejercicio9 de HDFS, usa la acción saveAsTextFile:
filter_rdd.saveAsTextFile("hdfs://localhost:9000/ejercicios/ejercicio9")
// ¿Cuántos ficheros se han creado en dicha carpeta? Explica por qué se ha creado ese número de ficheros.
4, porque el último stage del Job se ha ejecutado en 4 particiones, que corresponde al número de cores.
// ¿Cómo consultarías el contenido de todos los ficheros con un comando HDFS?
bin/hdfs dfs -cat /ejercicios/ejercicio9/part-00000
bin/hdfs dfs -cat /ejercicios/ejercicio9/part-00001
bin/hdfs dfs -cat /ejercicios/ejercicio9/part-00002
bin/hdfs dfs -cat /ejercicios/ejercicio9/part-00003

bin/hdfs dfs -cat /ejercicios/ejercicio9/part-0000*
