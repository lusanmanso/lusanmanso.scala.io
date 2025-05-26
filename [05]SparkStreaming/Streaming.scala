Ejercicios:

1) Tenemos un sistema de monitorización de una jornada de entrenamientos de Fórmula 1. En un punto dado del circuito, tenemos un radar de velocidad que nos arroja los siguientes datos:

EventID, Time, DriverID, Speed
------------------------------
0, 9:00.235, 7, 180
1, 9:00.237, 3, 185
2, 9:00.249. 4, 190
----
3, 9:00.450, 5, 192
----
4, 9:00.795, 8, 182
5, 9:00.815, 1, 198
6, 9:01.005, 6, 195
7, 9:01.204, 2, 184
---
8, 9:01.429, 9, 194
9, 9:01.626, 0, 199


Calcula el rango (para ello, indica almancena también valor máximo y mínimo) indicando Key, Max, Min y Range (Key=KeyId, Max=MaxNum, Min=MinNum, Range=MaxNum-MinNum) con los siguientes tipos de ventanas:

	a) Resolver con ventana temporal cada segundo (cada ventana empieza con el cambio de segundo)
		SOLUCIÓN:
			- Key=9:00.000, Max=198, Min=180, Range=18
			- Key=9:01.000, Max=199, Min=184, Range=15
	b) Resolver con ventana por número de eventos de tamaño 5
		SOLUCIÓN:
			- Key=9:00.235, Max=192, Min=180, Range=12
			- Key=9:00.815, Max=199, Min=184, Range=15
	c) Resolver con ventana deslizante de 200ms para calcular el rango del último segundo (primera ventana entre las 9:00.000 y las 9:01:00)
		SOLUCIÓN:
			- Key=9:00.000, Max=198, Min=180, Range=18
			- Key=9:00.200, Max=198, Min=180, Range=18
			- Key=9:00.400, Max=198, Min=182, Range=16
			- Key=9:00.600, Max=198, Min=182, Range=16
			- Key=9:00.800, Max=199, Min=184, Range=15
	d) Resolver con venta de sesión donde el periodo de inactividad es de 200ms
		SOLUCIÓN:
			- Key=9:00.235, Max=190, Min=180, Range=10
			- Key=9:00.450, Max=192, Min=192, Range=0
			- Key=9:00.795, Max=198, Min=182, Range=16
			- Key=9:01.429, Max=199, Min=194, Range=5

2) Tenemos un sistema de monitorización de una jornada de entrenamientos de Fórmula 1. En un momento dado, hay 20 coches en pista y cada uno está produciendo 50 eventos por segundo. Sin embargo, nuestro sistema de monitorización está desplegado con 2 nodos y solo es capaz de procesar 500 eventos por segundo. Propon 2 soluciones para que nuestro sistema no se sature y no se pierdan datos.

SOLUCIÓN:

	a) Backpressure: Acumular los eventos en una memoria incorporada en el sistema electrónico del coche de tal manera que solo envíe datos al sistema de monitorización cuando este lo pida.
	b) Escalabilidad: Desplegar 2 nodos más para que la capacidad del sistema sea de 1000 eventos por segundo.

3) Dí qué semántica de entrega es más conveniente para cada uno de estos escenarios:
	a) Monitorizar sensores de una estación metereológica.
		SOLUCIÓN: at-most-once
	b) Procesamiento de pedidos en tienda on-line.
		SOLUCIÓN: at-least-once
	c) Sistema de control de inventario de una farmacia.
		SOLUCIÓN: exactly-once
	d) Sitema de monitorización de piloto automático de avión
		SOLUCIÓN: exactly-once
	e) Detectar tendencias en redes sociales.
		SOLUCIÓN: at-most-once
	f) Alerta de ataque informático en CPD.
		SOLUCIÓN: at-least-once

4) Preguntas teóricas
	a) Si tenemos datos sobre ventas de una tienda on-line almacenados en HDFS, ¿cómo podríamos calcular la media de ventas por día con una arquitectura Lambda? ¿Y con una arquitectura Kappa?
		SOLUCIÓN:
			- Lambda: utilizando el Procesamiento Batch (Batch Layer)
			- Kappa: utilizando alguna herramienta para volcar los datos en un sistema streaming del que esté leyendo el sistema Kappa. Los datos deben ser divididos por día, de tal forma que podríamos utilizar una ventana por sesión donde se podría introducir un lapso de tiempo entre los eventos de cada día
	b) Para un sistema de detección de incendios, ¿utilizarías una aproximación Microbatch o Real-time para sus sensores?
		SOLUCIÓN:
			Dado que se requiere una latencia baja y una respuesta inmediata, un sistema Real-Time es más adecuado para este caso de uso. Además, en este caso, no tiene sentido realizar ningún tipo de agregación, ya que se debe realizar una acción inmediata cuando el nivel de humo supere cierto umbral establecido.
	c) Tomando el caso de uso del ejercicio 1, explica cuando se producen los siguientes eventos:
		- Event time
			SOLUCIÓN: Instante en que el radar mide la velocidad
		- Ingestion time
			SOLUCIÓN: Instante en el que el sistema de ingesta recibe el evento tras el envío desde el radar
		- Processing time
			SOLUCIÓN: Instante en el que el sistema de procesamiento trata el evento como podría ser el inicio de una ventana

5) Realiza el ejercicio 1 con un sistema Real-Time con estado y qué harías para mejorar la tolerancia a fallos

0, 9:00.235, 7, 180 --> Estado: Max=180, Min=180, Range=0
1, 9:00.237, 3, 185 --> Estado: Max=185, Min=180, Range=5
2, 9:00.249. 4, 190 --> Estado: Max=190, Min=180, Range=10
3, 9:00.450, 5, 192 --> Estado: Max=192, Min=180, Range=12
4, 9:00.795, 8, 182 --> Estado: Max=192, Min=180, Range=12
5, 9:00.815, 1, 198 --> Estado: Max=198, Min=180, Range=18
6, 9:01.005, 6, 195 --> Estado: Max=198, Min=180, Range=18
7, 9:01.204, 2, 184 --> Estado: Max=198, Min=180, Range=18
8, 9:01.429, 9, 194 --> Estado: Max=194, Min=180, Range=18
9, 9:01.626, 0, 199 --> Estado: Max=199, Min=180, Range=19

Checkpoint para persistir estado en una base de datos externa
