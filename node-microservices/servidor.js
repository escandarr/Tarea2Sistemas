const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka, logLevel } = require('kafkajs');
const PROTO_PATH = './proto/pedido.proto';

const brokersList = ['localhost:9092', 'localhost:9094', 'localhost:9096'];

// Cargar el archivo .proto
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const pedidoProto = grpc.loadPackageDefinition(packageDefinition).pedido;

// Crear el servidor gRPC
const server = new grpc.Server();

// Configurar el productor de Kafka conectado a todos los brokers
let kafkaProducer;

async function createKafkaProducer() {
  const kafka = new Kafka({
    clientId: 'gestion-pedidos',
    brokers: brokersList,  // Conectar a todos los brokers en lugar de uno solo
    logLevel: logLevel.ERROR,
  });

  kafkaProducer = kafka.producer();
  await kafkaProducer.connect();
  console.log(`Conectado a los brokers de Kafka: ${brokersList.join(', ')}`);
}

// Enviar pedido a Kafka
async function enviarPedidoKafka(pedido) {
  if (!kafkaProducer) {
    await createKafkaProducer();
  }

  try {
    await kafkaProducer.send({
      topic: 'pedidos',
      messages: [{ value: JSON.stringify(pedido) }],
    });
    console.log(`Pedido enviado a Kafka al tópico 'pedidos'`);
  } catch (error) {
    console.error('Error enviando el pedido a Kafka:', error);
  }
}

// Implementar la lógica del servicio gRPC
async function procesarPedido(call, callback) {
  const pedido = {
    email_cliente: call.request.email_cliente,
    producto: call.request.producto,
    precio: call.request.precio,
    pasarela_pago: call.request.pasarela_pago,
    banco: call.request.banco,
    marca_tarjeta: call.request.marca_tarjeta,
    direccion: call.request.direccion,
    numero: call.request.numero,
    region: call.request.region,
  };

  console.log('Pedido recibido:', pedido);
  await enviarPedidoKafka(pedido);

  callback(null, { mensaje: 'Pedido procesado correctamente', exito: true });
}

// Agregar el servicio al servidor gRPC
server.addService(pedidoProto.PedidoService.service, { RealizarPedido: procesarPedido });

// Iniciar el servidor gRPC en el puerto 50051
server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), (error, port) => {
  if (error) {
    console.error('Error al iniciar el servidor gRPC:', error);
    return;
  }
  console.log(`Servidor gRPC ejecutándose en el puerto ${port}`);
});
