>>> TEORÍA:

La diferencia clave entre un Future y un Promise en este contexto es el control sobre la ejecución y el resultado de la operación asincrónica.

1) Future: Representa una Computación en Curso (No Controlable)
- Un Future en Scala representa una operación que ya ha sido iniciada y que se completará en algún momento con un resultado exitoso o con un error.
- Una vez que el Future ha comenzado, no podemos modificar su resultado, solo observarlo cuando se complete.
- En el ejemplo:

  def calcularResultado(): Future[Int] = Future {
    Thread.sleep(5000)
    33
  }

  - La computación comienza de inmediato en un hilo separado y se completará automáticamente después de 5 segundos.
  - No podemos interferir en el resultado de este Future, solo reaccionar cuando termine.

2) Promise: Nos Permite Controlar Manualmente el Futuro
- Un Promise en Scala nos permite crear un Future sin que este se ejecute inmediatamente.
- El Promise nos da el control de cuándo y con qué valor completamos el Future.
- En el ejemplo:

  val promesaResultado = Promise[Int]()
  val futuroResultado: Future[Int] = promesaResultado.future

  - Aquí, el Promise genera un Future, pero este no tiene valor hasta que se asigne manualmente con .success(99).

  promesaResultado.success(99)

  - El Future asociado al Promise solo se completará cuando nosotros decidamos.

Inicio de ejecución:
  - Future: Se ejecuta automáticamente al crearse.
  - Promise: Se completa manualmente con success o failure.
Control sobre el resultado:
  - Future: No podemos modificar su valor.
  - Promise: Podemos definir cuándo y con qué valor se completa.
Uso recomendado
  - Future: Para tareas asincrónicas estándar.
  - Promise: Cuando necesitamos completar un Future manualmente.

Ejemplo de uso típico
  - Future: Ideal cuando no necesitamos intervenir en la ejecución (ej. llamada a una API externa).
  - Promise: Útil cuando queremos decidir el resultado en otro momento o desde otro hilo (ej. recibir datos de múltiples fuentes y elegir cuándo completarlo).

En resumen:
 - Future: es para computaciones que se ejecutan automáticamente.
 - Promise: nos da control sobre cuándo y cómo se completa un Future.

>>> EJEMPLO:
En este ejercicio, los alumnos trabajarán con Futures y Promises en Scala para manejar operaciones asincrónicas.

Objetivo:
- Implementar una función que devuelva un Future con un cálculo simulado que tarde unos segundos en completarse.
- Utilizar un Promise para completar un Future de manera manual en otro punto del código.
- Mostrar los valores resultantes cuando los Future se completen.
Tareas:
- Implementa una función calcularResultado() que retorne un Future[Int] tras un retraso de 5 segundos.
- Captura el resultado utilizando foreach y muestra el valor en pantalla.
- Crea una Promise[Int], asóciala a un Future y complétala con un valor específico.
- Observa el comportamiento al imprimir los valores obtenidos de ambos Future.

¿Qué pasaría si la lista de entrada no contiene números pares?

CÓDIGO:

// Futuros
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

def calcularResultado(): Future[Int] = Future {
  Thread.sleep(5000)
  33
}

val resultadoFuture: Future[Int] = calcularResultado()

resultadoFuture.foreach { resultado =>
  println(s"El resultado del futuro es: $resultado")
}

// Promesas
import scala.concurrent.Promise
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

val promesaResultado = Promise[Int]()

val futuroResultado: Future[Int] = promesaResultado.future

futuroResultado.foreach { resultado =>
  println(s"El resultado del futuro de la promesa es: $resultado")
}

promesaResultado.success(99)

>>> EJERCICIO: Procesamiento de Números con Promises, Filter y Reduce

Un sistema de procesamiento de datos recibe una lista de números enteros y debe realizar las siguientes operaciones de manera asincrónica:

1. Filtrar los números pares de la lista.
2. Sumar todos los números filtrados usando reduce.
3. Completar un Promise[Int] con el resultado de la suma.
4. Mostrar el resultado una vez que el Future del Promise se complete.

Requisitos:
- Usa un Promise[Int] para gestionar el resultado de forma manual.
- Utiliza filter para seleccionar solo los números pares.
- Usa reduce para calcular la suma total de los números filtrados.
- Muestra el resultado una vez que el Future de la Promise se complete.

Ejemplo de Entrada:

val numeros = List(10, 15, 22, 33, 40, 55)

Salida Esperada:

La suma de los números pares es: 72

SOLUCIÓN:

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global

def procesarNumeros(numeros: List[Int]): Future[Int] = {
  val promesa = Promise[Int]()

  Future {
    val numerosPares = numeros.filter(_ % 2 == 0) // Filtrar números pares
    val suma = numerosPares.reduceOption(_ + _).getOrElse(0) // Sumar pares o devolver 0 si la lista está vacía
    promesa.success(suma) // Completar el Promise con el resultado
  }

  promesa.future
}

// Lista de números de ejemplo
val numeros = List(10, 15, 22, 33, 40, 55)

// Ejecutar la función y manejar el resultado
procesarNumeros(numeros).foreach { resultado =>
  println(s"La suma de los números pares es: $resultado")
}
