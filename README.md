# wom_n3
para que este script funcione debe ser creada en primer lugar una instancia de composer en gcp, luego de eso crear un bucket en cloud storage donde se debe depositar el archivo CSV.
Posteriomente se debe crear un dataset en bigquery que se utilizara como repositorio de la data que se procesara del archivo CSV
Posteriormente se debe dejar el script de python en el bucket que se creo al momento de generar la instancia de composer, eso gatillara la ejecución del pipeline de composer y las tablas de Bigquery seran cargadas.
