Ejercicio 1
-----------

object TransformacionConFoldLeft extends App {

  // Función que realiza las operaciones especificadas utilizando foldLeft
  def transformarLista[A, B](lista: List[A], funcion: A => B): Map[A, B] = {
    val resultado = lista
      .reverse // Invertir el orden de la lista
      .foldLeft(Map.empty[A, B]) { (acumulador, elemento) =>
        acumulador + (elemento -> funcion(elemento))
      }
    resultado
  }

  // Lista de ejemplo
  val listaNumeros = List(1, 2, 3, 4, 5)

  // Función de ejemplo: elevar al cuadrado
  def elevarAlCuadrado(numero: Int): Int = numero * numero

  // Aplicar la transformación y mostrar el resultado
  val resultadoTransformacion = transformarLista(listaNumeros, elevarAlCuadrado)
  println(s"Resultado de la transformación: $resultadoTransformacion")
}

Ejercicio 2
-----------
// Definición de la case class Persona
case class Persona(nombre: String, edad: Int, email: String)

object AgrupacionPersonasPorEdad extends App {

  // Generación automática de 10 instancias de Persona con nombres consecutivos
  val personas: List[Persona] = (1 to 10).map(i => Persona(s"Persona $i", i % 5 + 20, s"email$i@example.com")).toList

  // Implementación 1: Utilizando groupBy
  def agruparPersonasPorEdad(personas: List[Persona]): Map[Int, List[Persona]] = {
    personas.groupBy(_.edad)
  }

  // Implementación 2: Utilizando foldLeft de manera funcional
  def agruparPersonasPorEdadFuncional(personas: List[Persona]): Map[Int, List[Persona]] = {
    personas.foldLeft(Map.empty[Int, List[Persona]]) { (acumulador, persona) =>
      val edad = persona.edad
      acumulador + (edad -> (acumulador.getOrElse(edad, List()) :+ persona))
    }
  }

  // Mostrar el resultado de ambas implementaciones
  val resultadoGroupBy = agruparPersonasPorEdad(personas)
  val resultadoFuncional = agruparPersonasPorEdadFuncional(personas)

  println("Resultado utilizando groupBy:")
  println(resultadoGroupBy)

  println("\nResultado utilizando foldLeft de manera funcional:")
  println(resultadoFuncional)
}
