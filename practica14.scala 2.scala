scala> 

scala> // 4. Mostramos el resultado

scala> println("\n--- VENTAS TOTALES POR CIUDAD (ORDEN ASCENDENTE) ---")

--- VENTAS TOTALES POR CIUDAD (ORDEN ASCENDENTE) ---

scala> rankingAsc.collect().foreach { case (ciudad, total) =>
     |   println(f" Ciudad: $ciudad%-10s -> Total: $total%.2f €")
     | }
 Ciudad: Sevilla    -> Total: 7,90 ?
 Ciudad: Valencia   -> Total: 7,95 ?
 Ciudad: Barcelona  -> Total: 8,65 ?
 Ciudad: Madrid     -> Total: 9,65 ?

scala> // 1. Extraemos la ciudad de cada línea (está en la posición 1 después del split)

scala> val listaDeCiudades = transacciones.map(linea => linea.split("\\|")(1))
val listaDeCiudades: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[23] at map at <console>:1

scala> 

scala> // 2. Contamos cuántas veces aparece cada ciudad

scala> val conteo = listaDeCiudades.map(ciudad => (ciudad, 1)).reduceByKey(_ + _)
val conteo: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[25] at reduceByKey at <console>:1

scala> 

scala> // 3. Filtramos: nos quedamos solo con las que aparecen más de 1 vez

scala> val ciudadesRepetidas = conteo.filter { case (ciudad, veces) => veces > 1 }
val ciudadesRepetidas: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[26] at filter at <console>:1

scala> 

scala> // 4. Imprimimos el resultado final

scala> println("Ciudades repetidas en las transacciones:")
Ciudades repetidas en las transacciones:

scala> ciudadesRepetidas.collect().foreach { case (ciudad, veces) => 
     |   println(s"-> $ciudad (aparece $veces veces)")
     | }
-> Valencia (aparece 6 veces)
-> Barcelona (aparece 6 veces)
-> Sevilla (aparece 6 veces)
-> Madrid (aparece 7 veces)

// 1. Filtramos y extraemos (Producto, 1) directamente
val conteoMadrid = transacciones
  .map(_.split("\\|"))
  .filter(p => p(1) == "Madrid")
  .map(p => (p(2), 1)) // Aquí creamos la pareja (Producto, 1)
  .reduceByKey(_ + _)

// 2. Ordenamos por el número (que es el segundo valor: _._2)
val masRepetido = conteoMadrid.sortBy(_._2, false)

// 3. Sacamos el resultado
val top = masRepetido.first()
println(s"\n--- GANADOR EN MADRID ---")
println(s"El producto '${top._1}' es el más vendido con ${top._2} ventas.")

// Exiting paste mode... now interpreting.

--- GANADOR EN MADRID ---
El producto 'Leche Entera' es el más vendido con 2 ventas.
val conteoMadrid: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[35] at reduceByKey at <pastie>:6
val masRepetido: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[40] at sortBy at <pastie>:9
val top: (String, Int) = (Leche Entera,2)

scala> // 1. Extraemos el ID del empleado de cada línea (por ejemplo, la posición 5)

scala> val empleados = transacciones.map(linea => {
     |   val campos = linea.split("\\|")
     |   val idEmpleado = campos(5) // Ajusta el número según cuál sea la columna del emple
ado
     |   (idEmpleado, 1)
     | })
val empleados: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[41] at map at <console>:1

scala> 

scala> // 2. Sumamos las transacciones por cada empleado

scala> val conteoPorEmpleado = empleados.reduceByKey(_ + _)
val conteoPorEmpleado: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[42] at reduceByKey at <console>:1

scala> 

scala> // 3. Mostramos el resultado

scala> println("\n--- TRANSACCIONES POR EMPLEADO ---")

--- TRANSACCIONES POR EMPLEADO ---

scala> conteoPorEmpleado.collect().foreach { case (id, total) =>
     |   val texto = if (total == 1) "transacción" else "transacciones"
     |   println(s" Empleado: $id -> Realizó: $total $texto")
     | }
 Empleado: E03 -> Realizó: 4 transacciones
 Empleado: E15 -> Realizó: 4 transacciones
 Empleado: E04 -> Realizó: 3 transacciones
 Empleado: E16 -> Realizó: 2 transacciones
 Empleado: E07 -> Realizó: 4 transacciones
 Empleado: E08 -> Realizó: 2 transacciones
 Empleado: E11 -> Realizó: 3 transacciones
 Empleado: E12 -> Realizó: 3 transacciones







scala> val productosMadridBarca = transacciones
val productosMadridBarca: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[14] at parallelize at <console>:1

scala>   .map(_.split("\\|"))
val res52: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[114] at map at <console>:1

scala>   .filter(p => p(1) == "Madrid" || p(1) == "Barcelona")
val res53: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[115] at filter at <console>:1

scala>   .map(p => p(2))
val res54: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[116] at map at <console>:1

scala>   .distinct()
val res55: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[119] at distinct at <console>:1

scala> 

scala> // 4. Mostramos la lista

scala> println("\n--- PRODUCTOS DE MADRID Y BARCELONA ---")

--- PRODUCTOS DE MADRID Y BARCELONA ---

scala> productosMadridBarca.collect().foreach(println)
T01|Madrid|Leche Entera|Lácteos|1.20|E03
T02|Barcelona|Pan de Molde|Panadería|1.85|E07
T03|Valencia|Leche Entera|Lácteos|1.20|E11
T04|Madrid|Zumo de Naranja|Bebidas|2.40|E03
T05|Sevilla|Yogur Natural|Lácteos|0.75|E15
T06|Barcelona|Agua Mineral|Bebidas|0.60|E07
T07|Madrid|Cerveza Rubia|Bebidas|1.10|E04
T08|Valencia|Pan de Molde|Panadería|1.85|E12
T09|Sevilla|Leche Entera|Lácteos|1.20|E15
T10|Madrid|Yogur Natural|Lácteos|0.75|E03
T11|Barcelona|Zumo de Naranja|Bebidas|2.40|E08
T12|Valencia|Cerveza Rubia|Bebidas|1.10|E11
T13|Sevilla|Agua Mineral|Bebidas|0.60|E16
T14|Madrid|Leche Entera|Lácteos|1.20|E04
T15|Barcelona|Pan de Molde|Panadería|1.85|E07
T16|Valencia|Yogur Natural|Lácteos|0.75|E12
T17|Sevilla|Zumo de Naranja|Bebidas|2.40|E15
T18|Madrid|Agua Mineral|Bebidas|0.60|E03
T19|Barcelona|Leche Entera|Lácteos|1.20|E08
T20|Valencia|Pan de Molde|Panadería|1.85|E11
T21|Sevilla|Cerveza Rubia|Bebidas|1.10|E16
T22|Madrid|Zumo de Naranja|Bebidas|2.40|E04
T23|Barcelona|Yogur Natural|Lácteos|0.75|E07
T24|Valencia|Leche Entera|Lácteos|1.20|E12
T25|Sevilla|Pan de Molde|Panadería|1.85|E15

scala> // 1. Filtramos para dejar pasar solo Madrid o Barcelona

scala> // 2. Sacamos el producto (posición 2)

scala> // 3. Usamos distinct para que cada nombre salga una sola vez

scala> val productosMadridBarca = transacciones
val productosMadridBarca: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[14] at parallelize at <console>:1

scala>   .map(_.split("\\|"))
val res58: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[120] at map at <console>:1

scala>   .filter(p => p(1) == "Madrid" || p(1) == "Barcelona")
val res59: org.apache.spark.rdd.RDD[Array[String]] = MapPartitionsRDD[121] at filter at <console>:1

scala>   .map(p => p(2))
val res60: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[122] at map at <console>:1

scala>   .distinct()
val res61: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[125] at distinct at <console>:1





scala> val madrid = transacciones.map(_.split("\\|")).filter(_(1) == "Madrid").map(_(2)).distinct()
val madrid: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[143] at distinct at <console>:1

scala> 

scala> // 2. Extraemos productos de Barcelona (sin duplicados)

scala> val barcelona = transacciones.map(_.split("\\|")).filter(_(1) == "Barcelona").map(_(
2)).distinct()
val barcelona: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[149] at distinct at <console>:1

scala> 

scala> // 3. Unimos ambas listas y volvemos a quitar duplicados 

scala> // (por si un producto está en las dos ciudades, que solo salga una vez)

scala> val resultadoFinal = madrid.union(barcelona).distinct()
val resultadoFinal: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[153] at distinct at <console>:1

scala> 

scala> // 4. Mostramos solo los nombres

scala> println("\n--- PRODUCTOS DE MADRID Y BARCELONA ---")

--- PRODUCTOS DE MADRID Y BARCELONA ---

scala> resultadoFinal.collect().foreach(println)
Leche Entera
Yogur Natural
Zumo de Naranja
Agua Mineral
Cerveza Rubia
Pan de Molde



scala> // 1. Extraemos el par (Categoría, Precio) de cada línea

scala> val ventasPorCategoria = transacciones.map(linea => {
     |   val campos = linea.split("\\|")
     |   val categoria = campos(3)
     |   val precio = campos(4).toDouble
     |   (categoria, precio)
     | })
val ventasPorCategoria: org.apache.spark.rdd.RDD[(String, Double)] = MapPartitionsRDD[160] at map at <console>:1

scala> 

scala> // 2. Sumamos los precios agrupando por la categoría

scala> val facturacion = ventasPorCategoria.reduceByKey((a, b) => a + b)
val facturacion: org.apache.spark.rdd.RDD[(String, Double)] = ShuffledRDD[161] at reduceByKey at <console>:1

scala> 

scala> // 3. Mostramos el resultado con el formato de tu imagen

scala> println("\nFacturación por categoría:")

Facturación por categoría:

scala> facturacion.collect().foreach { case (cat, total) =>
     |   println(f"  $cat%-10s -> $total%.2f€")
     | }
  Panadería  -> 9,25?
  Lácteos    -> 10,20?
  Bebidas    -> 14,70?

scala> // 1. Extraemos el producto (posición 2) y usamos distinct para no repetir nombres

scala> val productosUnicos = transacciones
val productosUnicos: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[14] at parallelize at <console>:1

scala>   .map(linea => linea.split("\\|")(2))
val res89: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[162] at map at <console>:1

scala>   .distinct()
val res90: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[165] at distinct at <console>:1

scala> 

scala> // 2. Ordenamos alfabéticamente

scala> // 'identity' le dice a Spark que ordene usando el propio nombre del producto

scala> val productosAlfabeticos = productosUnicos.sortBy(identity)
val productosAlfabeticos: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[170] at sortBy at <console>:1

scala> 

scala> // 3. Imprimimos el resultado

scala> println("\nLista de productos (A-Z):")

Lista de productos (A-Z):

scala> productosAlfabeticos.collect().foreach(p => println(s" -> $p"))
 -> T01|Madrid|Leche Entera|Lácteos|1.20|E03
 -> T02|Barcelona|Pan de Molde|Panadería|1.85|E07
 -> T03|Valencia|Leche Entera|Lácteos|1.20|E11
 -> T04|Madrid|Zumo de Naranja|Bebidas|2.40|E03
 -> T05|Sevilla|Yogur Natural|Lácteos|0.75|E15
 -> T06|Barcelona|Agua Mineral|Bebidas|0.60|E07
 -> T07|Madrid|Cerveza Rubia|Bebidas|1.10|E04
 -> T08|Valencia|Pan de Molde|Panadería|1.85|E12
 -> T09|Sevilla|Leche Entera|Lácteos|1.20|E15
 -> T10|Madrid|Yogur Natural|Lácteos|0.75|E03
 -> T11|Barcelona|Zumo de Naranja|Bebidas|2.40|E08
 -> T12|Valencia|Cerveza Rubia|Bebidas|1.10|E11
 -> T13|Sevilla|Agua Mineral|Bebidas|0.60|E16
 -> T14|Madrid|Leche Entera|Lácteos|1.20|E04
 -> T15|Barcelona|Pan de Molde|Panadería|1.85|E07
 -> T16|Valencia|Yogur Natural|Lácteos|0.75|E12
 -> T17|Sevilla|Zumo de Naranja|Bebidas|2.40|E15
 -> T18|Madrid|Agua Mineral|Bebidas|0.60|E03
 -> T19|Barcelona|Leche Entera|Lácteos|1.20|E08
 -> T20|Valencia|Pan de Molde|Panadería|1.85|E11
 -> T21|Sevilla|Cerveza Rubia|Bebidas|1.10|E16
 -> T22|Madrid|Zumo de Naranja|Bebidas|2.40|E04
 -> T23|Barcelona|Yogur Natural|Lácteos|0.75|E07
 -> T24|Valencia|Leche Entera|Lácteos|1.20|E12
 -> T25|Sevilla|Pan de Molde|Panadería|1.85|E15

scala> // 1. Cogemos solo el nombre del producto (posición 2) y quitamos repetidos

scala> val soloNombres = transacciones.map(_.split("\\|")(2)).distinct()
val soloNombres: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[174] at distinct at <console>:1

scala> 

scala> // 2. Ordenamos de la A a la Z

scala> val listaOrdenada = soloNombres.sortBy(identity)
val listaOrdenada: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[179] at sortBy at <console>:1

scala> 

scala> // 3. Imprimimos la lista limpia

scala> println("\nPRODUCTOS (ORDEN ALFABÉTICO):")

PRODUCTOS (ORDEN ALFABÉTICO):

scala> listaOrdenada.collect().foreach(p => println(p))
Agua Mineral
Cerveza Rubia
Leche Entera
Pan de Molde
Yogur Natural
Zumo de Naranja


scala> EJERCICIO 2 DEL TEMARIO 14
En el ejercicio cuando no se usa cache tiene que leer la lista de nuevo ,volver a comper con cada línea que con el split ("\\|") y volveer a convertir los pecios a toDoublee.Pero con el caché va más rápido ya que guarda en la memoria RAM el resultado .

El cache vendria boen si el linaje se bifurca es decir ; si los ingresoslimpios lo utilizariamos para usar el total,luego el promedio y después para un grafico .Asi spark corta el linaje ,guarda el resultado en la RAM y no tiene porque ejecutarlo 
No ,no es buena idea cachear tododos los RDDS .Ya que se llenaria la memoria RAM innecesariamente y , en muchos casos,acabaía ralentizando el sistema más de lo que ayuda 

En la ultima pregunta del ejercicio si tienes valores mas grandes que el cache() no los puede asumir se utilizaría
MEMORY_AND_DISK actua como sistema híbrido.Spark intentará guardar todo en la RAM y lo que no quepa lo guardara en el disco duro en vez de eleiminarlo o decartarlo .