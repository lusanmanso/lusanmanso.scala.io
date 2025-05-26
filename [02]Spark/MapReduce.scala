>>> ENUNCIADO:

Tenemos logs del servidor que aloja nuestra página web


- Queremos un análisis sobre la actividad de los usuarios en nuestra web
- Necesitamos extraer los clicks de cada sección, agrupándolos por hora
- Formato: fecha(dia-mes-año-hora:minuto:segundo), usuario, sección, sistema operativo
- Tenemos 3 datanodes (paralelismo)

Calcula las fases de map, shuffle y reduce

Split 1 --> 17-01-2015-15:21:34, pmadrigal, bicicletas, Mac
Split 1 --> 17-01-2015-15:34:53, darroyo, balones, Windows
Split 1 --> 17-01-2015-16:19:09, rvasallo, canastas, Android

Split 2 --> 17-01-2015-16:21:32, darroyo, balones, Windows
Split 2 --> 17-01-2015-16:35:02, rvasallo, balones, Android
Split 2 --> 17-01-2015-17:03:52, pmadrigal, balones, Mac

Split 3 --> 17-01-2015-17:22:23, rvasallo, canastas, Android
Split 3 --> 17-01-2015-17:36:12, darroyo, bicicletas, Windows
Split 3 --> 17-01-2015-17:39:59, darroyo, bicicletas, Windows

>>> SOLUCIÓN:

Pista: ¿Cómo resolverías este problema con una sentencia SQL? SELECT COUNT(*) FROM web_clicks GROUP BY hora, seccion

- FASE MAP (Formato: key, value):

Split 1:
17-01-2015-15:21:34, pmadrigal, bicicletas, Mac ==> (17-01-2015-15_bicicletas, 1)
17-01-2015-15:34:53, darroyo, balones, Windows ==> (17-01-2015-15_balones, 1)
17-01-2015-16:19:09, rvasallo, canastas, Android ==> (17-01-2015-16_canastas, 1)

Split 2:
17-01-2015-16:21:32, darroyo, balones, Windows ==> (17-01-2015-16_balones, 1)
17-01-2015-16:35:02, rvasallo, balones, Android ==> (17-01-2015-16_balones, 1)
17-01-2015-17:03:52, pmadrigal, balones, Mac ==> (17-01-2015-17_balones, 1)

Split 3:
17-01-2015-17:22:23, rvasallo, canastas, Android ==> (17-01-2015-17_canastas, 1)
17-01-2015-17:36:12, darroyo, bicicletas, Windows ==> (17-01-2015-17_bicicletas, 1)
17-01-2015-17:39:59, darroyo, bicicletas, Windows ==> (17-01-2015-17_bicicletas, 1)

- FASE SHUFFLE (Tenemos 7 claves diferentes y 3 splits/particiones distintas. Por tanto, asignamos 2 claves a 2 particiones y 3 claves a 1 partición para tener un reparto balanceado. Además, intentaremos aprovechar la localidad del dato para evitar envíos de datos a través de la red. Formato: Key, Array de valores):

Split 1 (3 claves: 17-01-2015-15_bicicletas, 17-01-2015-15_balones, 17-01-2015-16_canastas):
(17-01-2015-15_bicicletas, [1])
(17-01-2015-15_balones, [1])
(17-01-2015-16_canastas, [1])

Split 2 (2 claves: 17-01-2015-16_balones, 17-01-2015-17_balones):
(17-01-2015-16_balones, [1, 1])
(17-01-2015-17_balones, [1])

Split 3 (2 claves: 17-01-2015-17_canastas, 17-01-2015-17_bicicletas):
(17-01-2015-17_canastas, [1])
(17-01-2015-17_bicicletas, [1, 1])

- FASE REDUCE (Al ser un contador de eventos, tan solo tenemos que sumar los valores del array de cada clave. Formato: key, value):

Split 1:
(17-01-2015-15_bicicletas, 1)
(17-01-2015-15_balones, 1)
(17-01-2015-16_canastas, 1)

Split 2 (2 claves: 17-01-2015-16_balones, 17-01-2015-17_balones):
(17-01-2015-16_balones, 2)
(17-01-2015-17_balones, 1)

Split 3 (2 claves: 17-01-2015-17_canastas, 17-01-2015-17_bicicletas):
(17-01-2015-17_canastas, 1)
(17-01-2015-17_bicicletas, 2)

- RESULTADO FINAL:

(17-01-2015-15_bicicletas, 1)
(17-01-2015-15_balones, 1)
(17-01-2015-16_canastas, 1)
(17-01-2015-16_balones, 2)
(17-01-2015-17_balones, 1)
(17-01-2015-17_canastas, 1)
(17-01-2015-17_bicicletas, 2)
