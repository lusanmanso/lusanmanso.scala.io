>>> Ejercicio 3
-----------
- Lea un fichero de texto donde cada línea contiene la información de
una persona utilizando un separador especificado como parámetro
- Codificar una función que transforme una línea de fichero en una
Persona
- Filtre aquellas personas mayores de 18 y cuyo correo termine en
extensión .com
- Escriba el resultado en un fichero de texto con el mismo formato del
anterior.

Contenido del fichero datos_personas.txt:

Juan Perez,25,juan.perez@gmail.com
Maria Lopez,17,maria.lopez@yahoo.es
Carlos Gomez,30,carlos.gomez@hotmail.com
Ana Martinez,19,ana.martinez@outlook.com
Pedro Sanchez,16,pedro.sanchez@gmail.com
Luisa Fernandez,22,luisa.fernandez@gmail.com
Ricardo Diaz,35,ricardo.diaz@empresa.com
Marta Suarez,20,marta.suarez@correo.org
Esteban Castro,40,esteban.castro@yahoo.com
Laura Ramos,28,laura.ramos@dominio.com

-----------
import scala.io.Source
import java.io.{PrintWriter, File}

// Definición de la case class Persona
case class Persona(nombre: String, edad: Int, email: String)

object FiltradoPersonas extends App {

  // Función para transformar una línea de fichero en una Persona
  def lineaToFilaPersona(linea: String, separador: String): Persona = {
    val Array(nombre, edadStr, email) = linea.split(separador)
    Persona(nombre, edadStr.toInt, email)
  }

  // Función para filtrar personas mayores de 18 con correo que termine en .com
  def filtrarPersonas(personas: List[Persona]): List[Persona] = {
    personas.filter(persona => persona.edad > 18 && persona.email.endsWith(".com"))
  }

  // Leer el fichero de entrada
  val nombreFicheroEntrada = "datos_personas.txt"
  val separador = ","

  val lineasFichero = Source.fromFile(nombreFicheroEntrada).getLines().toList

  // Transformar las líneas del fichero en instancias de Persona
  val personas = lineasFichero.map(lineaToFilaPersona(_, separador))

  // Filtrar las personas según el criterio especificado
  val personasFiltradas = filtrarPersonas(personas)

  // Escribir el resultado en un nuevo fichero de texto
  val nombreFicheroSalida = "personas_filtradas.txt"

  val writer = new PrintWriter(new File(nombreFicheroSalida))
  personasFiltradas.foreach(persona => writer.println(s"${persona.nombre}$separador${persona.edad}$separador${persona.email}"))
  writer.close()

  println("Proceso completado. Resultado escrito en personas_filtradas.txt")
}


>>> EJERCICIO 4

Se requiere desarrollar un programa en Scala para procesar una lista de alumnos y calcular su calificación final. Para ello, sigue estos pasos:

1. Definir una case class llamada Alumno, que contenga los siguientes atributos:
   - nombre: String
   - notas: List[Int] (lista de calificaciones del alumno)

2. Implementar una función que reciba un Option[Alumno] y devuelva su calificación final, calculada como el promedio de sus notas. Si el alumno no existe (None), debe devolver 0 utilizando getOrElse.

3. Utilizar foldLeft para calcular la suma de las calificaciones antes de obtener el promedio.

4. Definir una función que tome otra función como parámetro y la aplique a un Alumno. Por ejemplo, una función que reciba una función de transformación y la aplique a la lista de calificaciones.

5. Implementar un pattern matching sobre el número de notas del alumno para clasificarlo de la siguiente manera:
   - Si no tiene notas, imprimir "Alumno sin calificaciones".
   - Si tiene una sola nota, imprimir "Alumno con evaluación única".
   - Si tiene más de una, imprimir "Alumno con múltiples calificaciones".

--- SOLUCIÓN

import scala.util.Random

// Definición de la case class Alumno
case class Alumno(nombre: String, notas: List[Int])

object ProcesadorAlumnos extends App {

  // Función para calcular el promedio de notas de un alumno usando Option y getOrElse
  def calcularPromedio(alumnoOpt: Option[Alumno]): Double = {
    alumnoOpt.map(alumno =>
      alumno.notas.foldLeft(0)(_ + _) / alumno.notas.length.toDouble
    ).getOrElse(0.0) // Si el alumno es None, devuelve 0.0
  }

  // Función de orden superior que aplica una transformación a la lista de notas
  def transformarNotas(alumno: Alumno, transformacion: List[Int] => List[Int]): Alumno = {
    alumno.copy(notas = transformacion(alumno.notas))
  }

  // Función para clasificar a los alumnos según la cantidad de notas usando pattern matching
  def clasificarAlumno(alumno: Alumno): String = {
    alumno.notas match {
      case Nil         => s"${alumno.nombre} es un Alumno sin calificaciones"
      case _ :: Nil    => s"${alumno.nombre} es un Alumno con evaluación única"
      case _           => s"${alumno.nombre} es un Alumno con múltiples calificaciones"
    }
  }

  // Ejemplo de uso con algunos alumnos
  val alumnos = List(
    Alumno("Carlos", List(90, 80, 85)),
    Alumno("Laura", List(100)),
    Alumno("Ana", List()),
    Alumno("Pedro", List(78, 85, 92, 88))
  )

  // Aplicar las funciones a cada alumno
  alumnos.foreach { alumno =>
    println(s"${alumno.nombre} tiene un promedio de: ${calcularPromedio(Some(alumno))}")
    println(clasificarAlumno(alumno))
  }

  // Ejemplo de uso de la función de orden superior para incrementar notas
  val alumnosTransformados = alumnos.map(alumno => transformarNotas(alumno, _.map(_ + 5)))

  println("\nNotas después de aplicar bonificación de 5 puntos:")
  alumnosTransformados.foreach(alumno => println(s"${alumno.nombre}: ${alumno.notas}"))
}
