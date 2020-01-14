# Flume Cacheable Interceptor

Es un componente Interceptor de Apache Flume que enriquece en base a un criterio de selección de campos alojados en un
archivo externo en formato CSV. El dato insertado es cacheado y la cache puede ser desactivada bajo demanda, por ejemplo,
 cuando el contenido del archivo externo ha cambiado.


## Configuración

Propiedades a añadir en el archivo de configuración de flume:

```ini
# Interceptor
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = org.apache.flume.interceptor.CacheableInterceptor$Builder

# Cache eviction
# interval is expressed in seconds, default to 10 seconds
a1.sources.r1.interceptors.i1.properties.lock.filename = /var/tmp/flume-cache.lock
a1.sources.r1.interceptors.i1.properties.lock.interval = 3600

# Data insertion
# CSV separator defaults to comma ","
a1.sources.r1.interceptors.i1.properties.csv.directory = /opt/myfiles
a1.sources.r1.interceptors.i1.properties.csv.separator = ,
# A map of criteria where the criterion name is a user defined subproperty
a1.sources.r1.interceptors.i1.properties.selection.criteria.1 = mapping#1
a1.sources.r1.interceptors.i1.properties.selection.criteria.2 = mapping#2
……………………………………………………………….
a1.sources.r1.interceptors.i1.properties.selection.criteria.n = mapping#n

```


## Funcionamiento del interceptor

```ini

1. Dentro del fichero de configuración del agente(s) de segundo nivel, 
   en la sección de configuración del interceptor, el usuario podría
   especificar N properties adicionales.

2. Un mapeo tiene dos campos:
    2.a. criterio de selección: es la condición que se debe verificar en
        un registro del CSV para poder aplicar el filtrado. Este campo se
        compone de un conjunto de clave-valor.
    2.b. listado de los campos: los campos del CSV que hay que seleccionar
        para el enriquecimiento.  

3. Si el registro del CSV hace match con un criterio de selección, entonces
   propagamos los campos indicados en 2.b, en caso contrario propagamos
   todos los campos (*).

El modo de definir un mapeo es por medio de un JSON, por ejemplo:
{"key":{"Field1":"ValueforField1","Field2":"ValueforField2"},"values":["Field3","Field4","Field5"] }

El campo "key" indica el criterio de selección, el campo "values" indica el
listado de campos del CSV a seleccionar para enriquecer el evento.

En éste ejemplo (una vez que se haya encontrado un registro del CSV que haga
match con la clave procedente del interceptor de primer nivel) comprobamos
que el registro tenga un campo  "Field1" con valor "ValueforField1" y un campo
"Field2" con valor "ValueforField2". Si esta condición se verifica solo
 enriquecemos con los campos "Field3","Field4","Field5".

El conjunto de campos que componen el criterio de selección indicado en un mapeo
no tiene porqué corresponderse  con la clave indicada en el interceptor de primer
nivel. Puede ser el mismo, pero no tiene porqué. Además, la clave de identificación
indicada en el interceptor de primer nivel siempre se  propaga, independientemente
 de lo que diga el mapeo.

```






