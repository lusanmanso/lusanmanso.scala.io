>>> EJEMPLO: Procesamiento de Archivos de Texto en Scala

En este ejercicio, trabajarás con lectura y escritura de archivos en Scala utilizando la biblioteca scala.io.Source y la clase PrintWriter.

Tareas:
1. Leer un archivo de texto ubicado en la ruta /home/bigdata/microcuento.txt.
2. Obtener estadísticas sobre el contenido del archivo:
   - Número total de líneas.
   - Verificar si todas las líneas tienen más de 50 o 100 caracteres.
   - Buscar la existencia de ciertas palabras como "llaves" o "datos".
   - Obtener el tamaño de todas las líneas que comienzan con la letra "U".
   - Extraer líneas que contengan más de 20 caracteres distintos.
   - Convertir el archivo a un solo string con separadores de línea.
   - Obtener la primera línea de forma segura (si no hay líneas, devolver "No hay primera línea").
   - Relacionar las primeras 10 líneas con un rango de números usando zip.
3. Escribir en un nuevo archivo ubicado en "/home/bigdata/microcuento_uppercase.txt":
   - Convertir todas las líneas del texto original a mayúsculas.
   - Guardar el resultado en el nuevo archivo.

---

>>> SOLUCIÓN

// Lectura de ficheros
import scala.io.Source

val filenameIntput = "/home/bigdata/microcuento.txt"

val lines = Source.fromFile(filenameIntput).getLines()
lines.size
lines.size

val lines = Source.fromFile(filenameIntput).getLines.toList
lines.size
lines.size

lines.forall(_.size > 50)
lines.forall(_.size > 100)

lines.exists(_.contains("llaves"))
lines.exists(_.contains("datos"))

lines.collect { case line if line.startsWith("U") => line.size }

lines.takeWhile(_.distinct.size > 20)

lines.mkString(System.lineSeparator())

lines.headOption.getOrElse("No hay primera linea")

Range.inclusive(1,10).zip(lines).mkString(System.lineSeparator())

// Escritura de ficheros
import java.io.PrintWriter

val filenameOutput = "/home/bigdata/microcuento_uppercase.txt"
val printWriter = new PrintWriter(filenameOutput)

lines.foreach(line => printWriter.println(line.toUpperCase))

printWriter.close()
