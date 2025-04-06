>>> TRAIT vs ABSTRACT
Características principales de un Trait en Scala
1) Similar a una interfaz en Java, pero con la capacidad de incluir métodos implementados.
2) No pueden tener parámetros en el constructor (a diferencia de las clases).
3) Se pueden extender múltiples traits, lo que permite una especie de herencia múltiple sin sus problemas tradicionales.
4) Se pueden combinar con clases concretas o abstractas.
---
trait Volador {
  def volar(): Unit = println("Estoy volando")
}
class Pajaro extends Volador
val pajaro = new Pajaro()
pajaro.volar() // Salida: Estoy volando
---
---
abstract class Animal(val nombre: String) {
  def hacerSonido(): Unit // Método abstracto
}
class Perro(nombre: String) extends Animal(nombre) {
  def hacerSonido(): Unit = println(s"$nombre dice: Guau!")
}
val perro = new Perro("Max")
perro.hacerSonido() // Salida: Max dice: Guau!
---

>>> HERENCIA MÚLTIPLE
Supongamos que queremos definir una clase Pato, que puede nadar y volar.
En Scala, podemos lograr esto con múltiples traits, ya que una clase puede extender varios traits a la vez.
---
// Trait que define la capacidad de nadar
trait Nadador {
  def nadar(): Unit = println("Estoy nadando")
}

// Trait que define la capacidad de volar
trait Volador {
  def volar(): Unit = println("Estoy volando")
}

// Clase Pato que hereda ambos traits
class Pato extends Nadador with Volador {
  def hacerSonido(): Unit = println("Cuac cuac!")
}

// Prueba
val pato = new Pato()
pato.nadar()     // Salida: Estoy nadando
pato.volar()     // Salida: Estoy volando
pato.hacerSonido() // Salida: Cuac cuac!

---

EJERCICIO:
Vas a modelar un sistema de vehículos en Scala. Algunos vehículos pueden volar, otros pueden navegar y otros pueden conducir por tierra. Deberás utilizar traits para representar estas habilidades y clases para los diferentes tipos de vehículos. Además, cada vehículo debe tener un atributo: su nombre, y debe definir el tipo de combustible.
Genera las clases Avion y Anfibio y llama a los métodos correspondientes para obtener la siguiente salida:
> Boeing 747 usa Querosen
> Volando por el cielo
>
> AmphiCar usa Híbrido
> Conduciendo por la carretera
> Navegando en el agua


SOLUCIÓN:

// Traits que representan habilidades de los vehículos
trait Terrestre {
  def conducir(): Unit = println("Conduciendo por la carretera")
}

trait Acuatico {
  def navegar(): Unit = println("Navegando en el agua")
}

trait Aereo {
  def volar(): Unit = println("Volando por el cielo")
}

// Clase abstracta Vehiculo
abstract class Vehiculo(val nombre: String) {
  def tipoCombustible(): String
}

// Clases que extienden Vehiculo y combinan traits
class Avion(nombre: String) extends Vehiculo(nombre) with Aereo {
  def tipoCombustible(): String = "Querosen"
}

class Anfibio(nombre: String) extends Vehiculo(nombre) with Terrestre with Acuatico {
  def tipoCombustible(): String = "Híbrido"
}

// Programa principal
val avion = new Avion("Boeing 747")
println(s"${avion.nombre} usa ${avion.tipoCombustible()}")
avion.volar()

val anfibio = new Anfibio("AmphiCar")
println(s"${anfibio.nombre} usa ${anfibio.tipoCombustible()}")
anfibio.conducir()
anfibio.navegar()

BONUS:

def muevete(vehiculo: Vehiculo) = vehiculo match {
  case te: Terrestre => te.conducir()
  case ac: Acuatico => ac.navegar()
  case ae: Aereo => ae.volar()
  case _ => println("Sin movimiento")
}
