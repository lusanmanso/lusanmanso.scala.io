// Append

def addAtTheEnd(list: List[Int], elem: Int): List[Int] = list :+ elem

addAtTheEnd(List(1, 2), 3)

addAtTheEnd(List(1, 2, 3), 3)

addAtTheEnd(List(1, 2, 3), 4)

// Prepend

def addAtTheBegin(list: List[Int], elem: Int): List[Int] = elem :: list

val myList = List(1, 2)

addAtTheBegin(myList, 0)

myList

>>> EJERCICIO: Añadir un Elemento a una Lista sin Duplicados
Escribe una función en Scala que agregue un elemento al final de una lista, pero solo si dicho elemento no está presente en la lista.

Requisitos:
1) Si el elemento ya existe, la lista debe mantenerse sin cambios.
2) Si el elemento no existe, debe agregarse al final de la lista.
3) La función debe recibir:
  - Una lista de enteros (List[Int]).
  - Un número entero (Int) a agregar.
4) Debe retornar una nueva lista con la posible modificación.

Ejemplo de entrada y salida esperada:

addAtTheEndIfNotExists(List(1, 2, 3), 3)  // Salida: List(1, 2, 3)
addAtTheEndIfNotExists(List(1, 2, 3), 4)  // Salida: List(1, 2, 3, 4)

Pista: Usa la función contains para verificar si el elemento ya está en la lista antes de agregarlo.


SOLUCIÓN:

def addAtTheEndIfNotExists(list: List[Int], elem: Int): List[Int] = {
  if (list.contains(elem))
    list
  else
    list :+ elem
}

addAtTheEndIfNotExists(List(1, 2, 3), 3)

addAtTheEndIfNotExists(List(1, 2, 3), 4)

// Merge

def mergeAtTheEnd(listA: List[Int], listB: List[Int]): List[Int] = listA ++ listB

val listA = List(1, 2)

val result = mergeAtTheEnd(listA, List(3, 4))

result

// Filter

Range(1, 10).foreach(println)

def doubleIfOdd(list: List[Int]): List[Int] = list.filter(_ % 2 != 0).map(_ * 2)

doubleIfOdd(Range(1, 10).toList)

// map

def doubleIfOddComplete(list: List[Int]): List[Int] = list.map(e => if (e%2 == 0) e else e * 2)

doubleIfOdd(Range.inclusive(1, 11).toList)

// flatMap

Range(1, 10).map { element =>
  Range.inclusive(1, 6, 2).map(_ * element)
}

Range(1, 10).flatMap { element =>
  Range.inclusive(1, 6, 2).map(_ * element)
}

// flatMap + Option

def dividir(a: Double, b: Int): Option[Double] = {
  if (b != 0) Some(a / b)
  else None
}

Range.inclusive(0, 100, 10).size
val result = Range.inclusive(0, 100, 10).reverse flatMap(dividir(100.0, _))
result.size

// Collections and pattern matching

def sum(ints: List[Int]): Int = {
  ints match {
    case Nil => 0
    case h :: Nil => h
    case h :: t => h + sum(t)
  }
}
sum(List())
sum(List(16))
sum(List(1, 2, 3))


>>> EJERCICIO: Eliminar los Primeros N Elementos de una Lista
Implementa una función en Scala que reciba una lista y un número entero n, y devuelva una nueva lista en la que se hayan eliminado los primeros n elementos.

Requisitos:
1) Si n es mayor o igual al tamaño de la lista, la función debe retornar una lista vacía.
2) Si n es 0 o menor, la función debe retornar la lista original sin cambios.
3) Debe utilizar pattern matching para manejar los casos de la lista vacía, lista con un solo elemento y lista con múltiples elementos.
4) La función debe ser recursiva.

Ejemplo de entrada y salida esperada:

drop(List(), 0)                // Salida: List()
drop(List(1), 5)               // Salida: List()
drop(List(1, 2, 5, 7), 3)      // Salida: List(7)
drop(List(10, 20, 30, 40), 2)  // Salida: List(30, 40)

Pista: Usa pattern matching para descomponer la lista en cabeza (h) y cola (t), y reduce n en cada llamada recursiva.

SOLUCIÓN:

def drop[A](list: List[A], n: Int): List[A] = {
  list match {
    case h :: t if n > 0 => drop(t, n - 1)
    case _ => list
  }
}
drop(List(), 0)
drop(List(1), 5)
drop(List(1, 2, 5, 7), 3)
drop(List(10, 20, 30, 40), 2)
