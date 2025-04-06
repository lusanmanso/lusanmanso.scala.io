>>> USO DE EJEMPLO

class Stack[T] {
  var elems: List[T] = Nil
  def push(x: T) { elems = x :: elems }
  def top: T = elems.head
  def pop() { elems = elems.tail }
}

---
// Crear una pila de enteros
val pila = new Stack[Int]
// Agregar elementos a la pila
pila.push(10)
pila.push(20)
pila.push(30)
// Ver el elemento del tope de la pila
println(pila.top)  // Imprime: 30
// Eliminar el elemento del tope
pila.pop()
// Ver el nuevo tope de la pila
println(pila.top)  // Imprime: 20
// Eliminar otro elemento
pila.pop()
// Ver el nuevo tope de la pila
println(pila.top)  // Imprime: 10
---
---
EJERCICIO: Realiza las mismas operaciones para una pila con "Hola", "Mundo", "Scala"
// Crear la pila para trabajar con Strings
val pilaDeStrings = new Stack[String]
// Agregar elementos a la pila
pilaDeStrings.push("Hola")
pilaDeStrings.push("Mundo")
pilaDeStrings.push("Scala")
// Ver el elemento del tope de la pila
println(pilaDeStrings.top)  // Imprime: Scala
// Eliminar el elemento del tope
pilaDeStrings.pop()
// Ver el nuevo tope de la pila
println(pilaDeStrings.top)  // Imprime: Mundo
// Eliminar otro elemento
pilaDeStrings.pop()
// Ver el nuevo tope de la pila
println(pilaDeStrings.top)  // Imprime: Hola
---

>>> EJEMPLO 1
Explicación:
- def intercambiar[A, B](valor1: A, valor2: B): Esta es una función genérica que toma dos valores, uno de tipo A y otro de tipo B. Los tipos A y B se determinan cuando se llama a la función.
- (valor2, valor1): La función devuelve una tupla con los valores intercambiados, primero de tipo B y luego de tipo A.
---
// Función genérica para intercambiar dos valores
def intercambiar[A, B](valor1: A, valor2: B): (B, A) = {
  (valor2, valor1)
}

// Intercambiamos dos enteros
val resultado1 = intercambiar(1, 2)
println(resultado1)  // Imprime: (2,1)

// Intercambiamos un string y un entero
val resultado2 = intercambiar("Scala", 42)
println(resultado2)  // Imprime: (42,Scala)
---

>>> EJEMPLO 2
---
class Box[T](content: T) {
  def getContent: T = content

  def map[U](f: T => U): Box[U] = {
    val mappedContent = f(content)
    new Box(mappedContent)
  }
}

val intBox = new Box(10)
println(intBox.getContent) // Imprime: 10

val stringBox = new Box("Scala")
println(stringBox.getContent)

val newStrBox = intBox.map((x: Int) => "a"*x)
println(newStrBox.getContent)

val newIntBox = stringBox.map((s: String) => s.length)
println(newIntBox.getContent)
---

EJERCICIO:
Vas a crear una clase genérica Caja[T] que pueda almacenar cualquier tipo de dato. La clase debe permitir:
- Guardar un valor de cualquier tipo.
- Obtener el valor almacenado.
- Actualizar el valor con uno nuevo.
- Mostrar un mensaje indicando el contenido de la caja cuando se imprima.

SOLUCIÓN:
// Definición de la clase genérica Caja[T]
class Caja[T](private var valor: T) {

  // Método para obtener el valor almacenado
  def obtener(): T = valor

  // Método para actualizar el valor almacenado
  def actualizar(nuevoValor: T): Unit = {
    valor = nuevoValor
  }

  // Sobrescribir toString para mostrar el contenido de la caja
  override def toString: String = s"Caja contiene: $valor"
}

// Programa principal
object GenericsEjemplo extends App {
  // Caja de enteros
  val cajaEntero = new Caja(5)

  // Caja de cadenas
  val cajaTexto = new Caja[String]("¡Hola, mundo!")
  println(cajaTexto)

  // Actualización de valores
  println("\nActualizando valores...\n")
  cajaEntero.actualizar(100)
  cajaTexto.actualizar("Adiós, mundo!")

  // Mostrar valores actualizados
  println(cajaEntero)
  println(cajaTexto)
}
