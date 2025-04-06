>>> EJEMPLO

val a: Option[Int] = Some(10)
// val a: Option[Int] = None
val b: Option[Int] = Some(5)

val resultado = for {
  x <- a
  y <- b
} yield x + y

>>> EJERCICIO

Dadas dos listas de números enteros:

val listaA = List(1, 2, 3)
val listaB = List(4, 5)

Utiliza un for-comprehension con yield para generar una nueva lista que contenga el producto de los elementos seleccionados, siguiendo estas reglas:
1) Solo se deben considerar los números impares de listaA.
2) Solo se deben considerar los números impares y positivos de listaA.
3) El resultado debe ser una lista con los productos de los números filtrados de listaA y listaB.

Ejemplo de salida esperada:
List(1*5, 3*5) // Resultado: List(5, 15)

Pista: Usa if dentro del for para aplicar los filtros necesarios.

SOLUCIÓN:

val listaA = List(1, 2, 3)
val listaB = List(4, 5)

val resultado = for {
  x <- listaA if x%2==1
  y <- listaB if y%2==1 && x>0
} yield x*y

println(resultado)
