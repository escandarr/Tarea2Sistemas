**Sistema de Gestión de Pedidos - Microservicio gRPC con Kafka**

Este proyecto es un microservicio para la gestión de pedidos que utiliza **gRPC** para recibir solicitudes de pedidos y **Kafka** para enviar mensajes a un tópico específico (pedidos). La arquitectura permite enviar los pedidos de manera cíclica entre tres brokers de Kafka para distribuir la carga.

### Estructura del Proyecto

- `servidor.js`: El servidor gRPC que escucha las solicitudes de pedidos y envía los mensajes a Kafka.
- `cliente.py`: Cliente gRPC en Python que lee un archivo CSV de pedidos y envía cada uno al servidor gRPC.
- `docker-compose.yml`: Archivo de configuración para desplegar Kafka, Zookeeper, Kafka UI, Elasticsearch y Kibana en contenedores Docker.
- `proto/pedido.proto`: Archivo de definición del servicio gRPC, donde se define el esquema del mensaje del pedido.

### Requisitos Previos

1. **Docker y Docker Compose**: Necesarios para ejecutar Kafka, Zookeeper y otros servicios.
2. **Node.js y npm**: Necesarios para ejecutar el servidor gRPC.
3. **Python**: Necesario para el cliente gRPC que envía los pedidos.
4. **Bibliotecas de Python**: Instalar las dependencias en `python-client/requirements.txt`.

### Configuración y Ejecución

#### 1. Configurar y Ejecutar Docker

Asegúrate de que los puertos definidos en `docker-compose.yml` están disponibles en tu sistema:
- Kafka Brokers: 9092, 9094, 9096
- Kafka UI: 8080
- Zookeeper: 2181
- Elasticsearch: 9200
- Kibana: 5601

Levanta todos los servicios ejecutando el siguiente comando en la raíz del proyecto:
```bash
docker-compose up -d
```

Verifica que los contenedores se estén ejecutando correctamente:
```bash
docker ps
```

Opcional: Usa Kafka UI (disponible en http://localhost:8080) para verificar que los brokers de Kafka están en funcionamiento y que el topic `pedidos` está configurado.

#### 2. Configuración del Servidor gRPC

El servidor gRPC en `servidor.js` está configurado para:
- Escuchar en el puerto `50051`.
- Recibir pedidos en formato definido en `pedido.proto`.
- Enviar cada pedido a uno de los brokers de Kafka de manera cíclica (`kafka1`, `kafka2`, `kafka3`).

Para ejecutar el servidor gRPC:
```bash
node servidor.js
```

Deberías ver mensajes de conexión exitosos con Kafka y mensajes de confirmación de envío en el terminal.

#### 3. Configuración del Cliente gRPC

El cliente en `cliente.py` lee un archivo CSV (`dataset_sales.csv`) y envía cada fila como un pedido al servidor gRPC. Cada fila del CSV debe contener los campos necesarios como `Product`, `Price`, `Email_Client`, entre otros.

Para ejecutar el cliente gRPC:
```bash
pip install -r requirements.txt
python cliente.py
```

El cliente enviará cada pedido al servidor gRPC, y en la terminal deberías ver una confirmación de que cada pedido fue procesado exitosamente.

#### 4. Verificación del Flujo de Datos

- Revisa los logs de `servidor.js` para confirmar que cada pedido fue enviado exitosamente al topic `pedidos` en Kafka.
- Abre Kafka UI para verificar los mensajes en el topic `pedidos`.

### Diagnóstico de Errores Comunes

- **Error `getaddrinfo EAI_AGAIN`**: Esto generalmente indica un problema de DNS o que Kafka no es accesible desde el servidor gRPC. Asegúrate de que los brokers de Kafka están funcionando y de que puedes hacer ping a `kafka1`, `kafka2`, y `kafka3` desde el contenedor del servidor gRPC.
- **No se envían los mensajes a Kafka**: Verifica que el topic `pedidos` esté correctamente configurado en Kafka. Puedes comprobarlo en Kafka UI y también revisar los logs de `servidor.js`.
- **Problemas de conexión con gRPC**: Asegúrate de que el puerto `50051` esté disponible y de que no hay conflictos de red. Revisa los logs tanto del cliente como del servidor.

### Funcionalidad Cíclica de Kafka

El servidor gRPC está configurado para enviar pedidos a los brokers de Kafka de manera cíclica, alternando entre `kafka1`, `kafka2`, y `kafka3`. Esto se logra mediante la función `getNextBroker()` en `servidor.js`, la cual selecciona el siguiente broker en la lista de brokers.

### Notas Adicionales

- **Seguridad**: Esta configuración utiliza listeners `PLAINTEXT` sin autenticación, lo cual no es seguro para entornos de producción.
- **Optimización**: En un entorno de producción, podrías configurar listeners adicionales o ajustar los parámetros de conexión y balanceo de carga.
---
