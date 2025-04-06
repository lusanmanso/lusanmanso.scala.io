// Side effects

import scala.util.{Failure, Success, Try}

def divisionWithException(x: Int, y: Int): Int = {
  try {
    x/y
  } catch {
    case e: Exception =>
      println("division entre 0")
      x
  }
}

divisionWithException(9, 0)

def divisionwithTryPM(x: Int, y: Int): Int = {
    Try(x/y) match {
      case Success(resultado) =>
        resultado
      case Failure(error) =>
        println(error)
        x
  }
}

divisionwithTryPM(9, 0)

def divisionwithTry(x: Int, y: Int): Try[Int] = {
  Try(x/y)
}

divisionwithTry(9, 0)

def divisionwithOption(x: Int, y: Int): Option[Int] = {
  Try(x/y) match {
    case Success(resultado) =>
      Some(resultado)
    case _ =>
      None
  }
}

divisionwithOption(9, 0).getOrElse(Int.MaxValue)


>>> EJERCICIO: Manejo de Excepciones con Try-Catch y Pattern Matching
Escribe una función en Scala que intente convertir un String a un número entero y maneje posibles errores utilizando la estructura Try-Catch.

Requisitos:
1) Usa Try para capturar la posible excepción al convertir el String a Int.
2) Utiliza pattern matching para manejar los casos:
  - Success(valor): Devuelve el número convertido.
  - Failure(error): Devuelve un mensaje indicando que la conversión falló.
3) Prueba la función con diferentes valores, incluyendo entradas válidas e inválidas.

Pista: Try captura excepciones automáticamente. Usa Success y Failure con pattern matching para manejar los resultados.

SOLUCIÓN:

import scala.util.{Try, Success, Failure}

def safeParseInt(str: String): String = {
  Try(str.toInt) match {
    case Success(value) => s"Conversión exitosa: $value"
    case Failure(exception) => s"Error: No se pudo convertir '$str' a un número."
  }
}

// Pruebas
println(safeParseInt("123"))  // Salida: Conversión exitosa: 123
println(safeParseInt("abc"))  // Salida: Error: No se pudo convertir 'abc' a un número.
println(safeParseInt("42a"))  // Salida: Error: No se pudo convertir '42a' a un número.
