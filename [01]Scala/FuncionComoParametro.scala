>>> EJEMPLO:
Define una función filtrarLista que reciba:
- Una lista de enteros (lista).
- Una función (criterio) que tome un entero y devuelva un Boolean (es decir, un predicado).
Luego, usa esta función para filtrar los números pares y los mayores que un cierto valor.
---
def filtrarLista(lista: List[Int], criterio: Int => Boolean): List[Int] = {
lista.filter(criterio)
}
// Definir funciones de filtrado
val esPar: Int => Boolean = x => x % 2 == 0
val mayorQueCinco: Int => Boolean = x => x > 5
// Lista de prueba
val numeros = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
// Uso de la función con distintos criterios
println(filtrarLista(numeros, esPar)) // Salida esperada: List(2, 4, 6, 8, 10)
println(filtrarLista(numeros, mayorQueCinco)) // Salida esperada: List(6, 7, 8, 9, 10)
---
>>> EJERCICIO:
Define una función operar que reciba:
- Dos números enteros (a y b).
- Una función (f) que tome dos enteros y devuelva un entero.
Luego, usa esta función para realizar distintas operaciones matemáticas: suma y multiplicación.
---
def operar(a: Int, b: Int, f: (Int, Int) => Int): Int = {
f(a, b)
}
// Definir funciones de suma y multiplicación
val suma = (x: Int, y: Int) => x + y
val multiplicacion = (x: Int, y: Int) => x * y
// Uso de la función operar con distintas funciones
println(operar(5, 3, suma)) // Salida esperada: 8
println(operar(5, 3, multiplicacion)) // Salida esperada: 15
---
