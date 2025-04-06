Uso de Implicits con Unidades de Medida
En este ejercicio, los estudiantes aprenderán a utilizar implicits para trabajar con unidades de medida, permitiendo realizar conversiones automáticas entre diferentes unidades de longitud, como metros y kilómetros.

Requisitos:
Definir un tipo Longitud:
Crea una clase Longitud que represente una medida de longitud en metros (Double).

Agregar conversiones implícitas:
Implementa una conversión implícita para transformar metros a kilómetros y viceversa.

Métodos adicionales mediante implicit class:
Crea un método adicional para la clase Longitud llamado aKilometros que convierta metros a kilómetros, y un método aMetros que convierta kilómetros a metros. Ambos métodos deben devolver una nueva instancia de Longitud con la unidad convertida.

Realizar pruebas:
Usa los métodos y conversiones implícitas para realizar operaciones entre diferentes unidades de longitud.

Pistas:
Utiliza implicit def para convertir entre Double (representando kilómetros) y Longitud (en metros).
Puedes realizar operaciones entre diferentes unidades utilizando las conversiones implícitas, sin necesidad de escribir código adicional para las conversiones.

SOLUCIÓN:

// 1. Clase Longitud que representa la medida en metros
class Longitud(val metros: Double) {
  def aKilometros: Longitud = new Longitud(metros / 1000)
  def aMetros: Longitud = new Longitud(metros * 1000)

  override def toString: String = s"$metros metros"
}

// 2. Conversión implícita de Kilómetros a Longitud (metros)
implicit def kmToLongitud(km: Double): Longitud = new Longitud(km * 1000)

// 3. Conversión implícita de Longitud (metros) a Kilómetros
implicit def longitudToKm(longitud: Longitud): Double = longitud.metros / 1000

// 4. Pruebas
val distanciaEnMetros = new Longitud(1500)
println(distanciaEnMetros.aKilometros)  // Salida: 1.5 metros en kilómetros
println(distanciaEnMetros.aMetros)      // Salida: 1500 metros

val distanciaEnKm: Longitud = 2.5 // Se convierte automáticamente a Longitud en metros
println(distanciaEnKm)  // Salida: 2500 metros

val distanciaEnMetros2: Double = distanciaEnMetros // Se convierte automáticamente a Double
println(distanciaEnMetros2)  // Salida: 1500.0 metros

Explicación de conceptos:

- Conversión implícita:
Convertir entre kilómetros y metros de manera automática utilizando implicit def. Cuando un número de tipo Double que representa kilómetros es utilizado en un contexto donde se espera un objeto Longitud, la conversión implícita lo transforma a metros.

- Métodos de conversión dentro de Longitud:
Los métodos aKilometros y aMetros permiten convertir de metros a kilómetros y viceversa dentro de la clase Longitud.
