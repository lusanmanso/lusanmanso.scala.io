>>> EJEMPLO
---
// Definimos la clase base
abstract class Expr
// Expresión para números
case class Number(n: Double) extends Expr
// Expresión para variables
case class Var(name: String) extends Expr
// Operaciones unarias (ej. negación)
case class UnOp(operator: String, arg: Expr) extends Expr
// Operaciones binarias (ej. suma, multiplicación)
case class BinOp(operator: String, left: Expr, right: Expr) extends Expr
---
---
val expr1 = UnOp("-", UnOp("-", Number(5))) // --5 → se debe simplificar a 5
val expr2 = BinOp("+", Var("x"), Number(0)) // x + 0 → se debe simplificar a x
val expr3 = BinOp("*", Var("y"), Number(1)) // y * 1 → se debe simplificar a y
val expr4 = BinOp("+", Number(3), Number(4)) // 3 + 4 → no se simplifica
println(simplifyTop(expr1)) // Salida: Number(5.0)
println(simplifyTop(expr2)) // Salida: Var(x)
println(simplifyTop(expr3)) // Salida: Var(y)
println(simplifyTop(expr4)) // Salida: BinOp(+, Number(3.0), Number(4.0))
---
>>> EJERCICIO
Se desea modelar un sistema que clasifique personas según su edad usando pattern matching en Scala.
Requerimientos:
1) Define una case class llamada Persona con dos atributos:
- nombre: String
- edad: Int
2) Implementa una función clasificarPersona(p: Persona): String que:
- Devuelva "Menor de edad" si la persona tiene menos de 18 años.
- Devuelva "Adulto" si la persona tiene entre 18 y 65 años.
- Devuelva "Adulto mayor" si la persona tiene más de 65 años.
3) Prueba la función con diferentes instancias de Persona.
---
case class Persona(nombre: String, edad: Int)
def describirPersona(p: Persona): String = p match {
case Persona(nombre, edad) if edad < 18 => s"$nombre es menor de edad."
case Persona(nombre, edad) if edad <= 65 => s"$nombre es adulto."
case other => s"${other.nombre} es adulto mayor."
}
val p1 = Persona("Juan", 16)
val p2 = Persona("Ana", 25)
val p3 = Persona("Sara", 70)
println(describirPersona(p1)) // Salida: Juan es menor de edad.
println(describirPersona(p2)) // Salida: Ana es adulto.
println(describirPersona(p3)) // Salida: Sara es adulto mayor.
---
