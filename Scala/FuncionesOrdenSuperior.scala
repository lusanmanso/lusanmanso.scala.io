>>> EJEMPLO map vs flatMap

// Lista de frases
val frases = List("Hola mundo", "Scala es genial", "La programación funcional es poderosa")

// Map: Usando map para transformar cada frase en una lista de palabras
val palabrasConMap = frases.map(frase => frase.split(" "))

// FlatMap: Usando flatMap para obtener una lista de todas las palabras
val palabrasConFlatMap = frases.flatMap(frase => frase.split(" "))

// Imprimir resultados
println("Resultado usando map:")
println(palabrasConMap)

println("\nResultado usando flatMap:")
println(palabrasConFlatMap)

>>> EJERCICIO

val numeros = List(1, 2, 3, 4, 5)

1) Usa map para transformar cada número en una lista que contenga el número y su doble.
(Por ejemplo: el número 2 se convierte en List(2, 4)).

2) Usa flatMap para obtener una única lista con todos los números y sus dobles.

SOLUCIÓN:

// Usando map para crear listas con el número y su doble
val resultadoMap = numeros.map(n => List(n, n * 2))

// Usando flatMap para obtener una única lista con todos los números y sus dobles
val resultadoFlatMap = numeros.flatMap(n => List(n, n * 2))

// Imprimir resultados
println("Resultado usando map:")
println(resultadoMap)

println("\nResultado usando flatMap:")
println(resultadoFlatMap)

>>> EJERCICIO Filter+Reduce
Obten el número impar más alto de esta lista:
val numeros = List(23, 44, 33, 87, 98)

SOLUCIÓN:
// Filtrar los números impares:
val numerosImpares = numeros.filter(_ % 2 != 0)
println(s"Números impares: $numerosImpares")  // Salida: List(23, 33, 87)

// Encontrar el número impar más alto utilizando reduce:
val maxImpar = numerosImpares.reduce((a, b) => if (a > b) a else b)
println(s"El número impar más alto es: $maxImpar")  // Salida: 87

>>> EJERCICIO: FoldLeft

Dada una lista de números enteros representados como Option[Int], escribe una función que sume todos los valores presentes (es decir, los que no son None) utilizando foldLeft.
Si todos los valores son None, el resultado debe ser 0.

val numeros: List[Option[Int]] = List(Option(5), None, Option(10), Option(3), None, Option(7))

val sumaTotal = numeros.foldLeft(0) { (acumulador, numeroOpt) =>
  numeroOpt match {
    case Some(valor) => acumulador + valor
    case None        => acumulador  // No sumamos nada si es None
  }
}

println(s"La suma total es: $sumaTotal")  // Salida esperada: 25
