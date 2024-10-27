// Dependencias necesarias
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka, Partitioners } = require('kafkajs'); // Librería de Kafka

// Ruta al archivo .proto
const PROTO_PATH = './proto/pedido.proto';

// Configuración de gRPC
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const pedidoProto = grpc.loadPackageDefinition(packageDefinition).pedido;

// Configuración de Kafka
const kafka = new Kafka({
  clientId: 'gestion-pedidos',
  brokers: ['kafka1:9092', 'kafka2:9094', 'kafka3:9096'] // Corregir los puertos si es necesario
});

const producer = kafka.producer();

// Inicialización del productor de Kafka
async function iniciarProductorKafka() {
  try {
    await producer.connect();
    console.log('Kafka productor conectado al broker');
  } catch (error) {
    console.error('Error al conectar el productor de Kafka:', error);
  }
}

// Función para enviar pedidos a Kafka
// Función para enviar pedidos a Kafka
const enviarPedidoKafka = async (pedido) => {
  try {
    if (!producer.isConnected()) {
      await producer.connect();
      console.log("Reconectado al broker de Kafka para pedidos");
    }
    await producer.send({
      topic: 'orders',
      messages: [{ value: JSON.stringify(pedido) }]
    });
    console.log('Pedido enviado a Kafka en el tópico orders');
  } catch (error) {
    console.error(`Error enviando el pedido a Kafka: ${error}`);
  }
};

// Función para enviar métricas a Kafka
const enviarMetricasKafka = async (metricas) => {
  try {
    if (!producer.isConnected()) {
      await producer.connect();
      console.log("Reconectado al broker de Kafka para métricas");
    }
    await producer.send({
      topic: 'metrics',
      messages: [{ value: JSON.stringify(metricas) }]
    });
    console.log('Métricas enviadas a Kafka en el tópico metrics');
  } catch (error) {
    console.error(`Error enviando métricas a Kafka: ${error}`);
  }
};


// Función para enviar métricas a Kafka
async function enviarMetricasKafka(metrica) {
  try {
    await producer.send({
      topic: 'metricas',
      messages: [{ value: JSON.stringify(metrica) }]
    });
    console.log('Métricas enviadas al tópico "metricas" en Kafka');
  } catch (error) {
    console.error('Error enviando métricas a Kafka:', error);
  }
}

// Implementación del servicio gRPC
function procesarPedido(call, callback) {
  const pedido = {
    producto: call.request.producto,
    precio: call.request.precio,
    email_cliente: call.request.email_cliente,
    pasarela_pago: call.request.pasarela_pago,
    banco: call.request.banco,
    marca_tarjeta: call.request.marca_tarjeta,
    direccion: call.request.direccion,
    numero: call.request.numero,
    region: call.request.region,
    timestamp: new Date().toISOString()
  };

  console.log(`Pedido recibido: ${pedido.producto} para ${pedido.email_cliente}`);

  // Enviar datos a Kafka
  enviarPedidoKafka(pedido);
  enviarMetricasKafka(pedido);

  // Respuesta al cliente gRPC
  callback(null, { mensaje: `Pedido procesado correctamente para ${pedido.email_cliente}`, exito: true });
}

// Configuración del servidor gRPC
function iniciarServidorGrpc() {
  const servidor = new grpc.Server();
  servidor.addService(pedidoProto.PedidoService.service, { RealizarPedido: procesarPedido });

  servidor.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), (error, puerto) => {
    if (error) {
      console.error('Error al iniciar el servidor gRPC:', error);
      return;
    }
    console.log(`Servidor gRPC escuchando en el puerto ${puerto}`);
    servidor.start();
  });
}

// Función principal para iniciar el servidor y el productor de Kafka
async function main() {
  await iniciarProductorKafka(); // Conectar a Kafka
  iniciarServidorGrpc(); // Iniciar servidor gRPC
}

// Iniciar la aplicación
main();
