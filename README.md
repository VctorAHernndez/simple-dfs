# README – Proyecto DFS
## Detalles Generales
**Autor**: Víctor A. Hernández Castro
**Colaborador**: José R. Ortiz Ubarri
	* El Dr. Ortiz nos proveyó un esqueleto del proyecto, al igual que una ilustración del funcionamiento del mismo, para nosotros entender mejor en qué consta el mismo y cómo tendríamos que implementarlo.
**Referencias**: 
	* [SQLite for Python 2.7](https://docs.python.org/2/library/sqlite3.html)
	* [JSON for Python 2.7](https://docs.python.org/2/library/json.html)
	* [UUID for Python 2.7](https://docs.python.org/2/library/uuid.html)
	* [SocketServer for Python 2.7](https://docs.python.org/2/library/socketserver.html)
	* [Sockets for Python 2.7](https://docs.python.org/2/library/socket.html)

**DISCLAIMER**: USE LOS FILES `mds_db.py` Y `Packet.py` PROVISTOS, YA QUE ESTOS ESTÁN “BUG-FREE” Y HACEN USO DE UNA FUNCIÓN “CUSTOM” ADICIONAL QUE SE HIZO (véase `getFileSize()`).

**DISCLAIMER**: HAGA LAS LLAMADAS `rm dfs.db` Y `python createdb.py` ANTES DE COMENZAR A USAR ESTOS SCRIPTS.
- - - -

## Funcionalidad
Estos scripts simulan el comportamiento de un Distributed File System (DFS) que yace en una máquina remota `meta-data.py` y se comunica con sus respectivos data nodes (`data-node.py`) para servirle a los clientes locales que lo procuren (`ls.py` o `copy.py`). Los mismos usan módulos construidos por el Dr. Ortiz, `Packet.py` y `mds_db.py`, que facilitan el envío y recibo de información a través de la red, como también proveen una medida de comunicación con la base de datos que maneja los inodes, los data nodes y los data blocks. Al correr, cada script (excepto `ls.py`) describe muy detalladamente (e.g. “verbose”) cada paso que hace al interactuar con los demás.



### Meta Data Server (MDS)
Para poder correr el MDS, simplemente se corre el siguiente comando desde el terminal:
	* `python meta-data.py <mds port, default=8000>`
Al correr, el MDS espera satisfacer los mensajes de sus clientes:
	* `data-node.py` usa `reg` para registrarse como data node en la base de datos
	* `ls.py` usa `list` para recibir la lista de files en el DFS
	* `copy.py` usa `put` para pedir insertar en el DFS, `get` para pedir un file del DFS y `dblks` para pedir notificar al MDS dónde se encuentran los data blocks acabados de guardar en los data nodes

### List Client
Para poder correr `ls.py`, simplemente se corre el siguiente comando desde el terminal (después de tener el MDS corriendo):
	* `python ls.py <mds ip>:<mds port, default=8000>`

### Data Nodes (DNs)
Para poder correr los DNs, simplemente se corre el siguiente comando desde el terminal (después de tener el MDS corriendo):
	* `python data-node.py <dn ip> <dn port> <data path> <mds port, default=8000>`
El `<data path>` es la dirección a un folder donde se puedan guardar los data blocks al recibirlos de `copy.py`. Esto mantiene las cosas recogidas.

### Copy Client
Para poder correr `copy.py`, simplemente se corre uno de los siguientes comandos desde el terminal, dependiendo de si se copia al DFS o si se copia del DFS (después de tener el MDS y los DNs corriendo):
	1. **Al DFS:**
		* `python copy.py <source> <mds ip>:<mds port>:<mds path>`
	2. **Del DFS:**
		* `python copy.py <mds ip>:<mds port>:<mds path> <dest>`



## Algunos Detalles de Implementación
	1. `copy.py` no permite copiar sobre files ya existentes, ni permite intentar copiar directorios, ni permite copiar files que no existen, ni permite copiar sobre directorios existentes…
	2. El algoritmo de división del file (por parte de `copy.py`) consta en tratar de dividir el archivo equitativamente entre los DNs disponibles. Para evitar cargar la memoria del programa (e.g. tener un buffer size muy grande de parte y parte), se envían en “subchunks” hasta completar lo que le llamé la “cuota” de cada DN. De esa manera, siempre tendremos que la cantidad de bloques por file va a ser igual a la cantidad de DNs disponibles al momento de ser subido el archivo.
	3. La comunicación entre los DNs y `copy.py` usa métodos “blocking” para asegurarse de que los mensajes no se corrompan al “colarse” los unos con los otros. Se implementó con un `recv(x)` pareado con un `send("MORE")`/`send("DONE")`/`send("OK")` de parte y parte.
	4. Todos los scripts funcionan con “absolute paths” solamente (no se garantiza que funcionen con “relative paths”)



## Variables Globales (modificables)
> copy.py  
* `SUBCHUNK_BUFFER` – must be the same as in `data-node.py`
* `DNODE_BUFFER` – must be big if using many DNs
* `CHUNKLIST_BUFFER` – must be big if using many DNs; must be the same as in `meta-data.py`

> data-node.py  
* `DATA_PATH` – global on purpose (initial value doesn’t matter)
* `SUBCHUNK_BUFFER` – must be the same as in `copy.py`

> meta-data.py  
* `CHUNKLIST_BUFFER` – must be big if using many DNs; must be the same as in `meta-data.py`

> ls.py  
* `LS_BUFFER` – must be big if inserting many files into DFS


