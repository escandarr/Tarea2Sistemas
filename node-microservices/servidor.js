const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka, Partitioners } = require('kafkajs');

// Ruta del archivo .proto
const PROTO_PATH = './proto/pedido.proto';

// Opciones para cargar el archivo .proto
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

// Cargar el paquete generado a partir del archivo .proto
const pedidoProto = grpc.loadPackageDefinition(packageDefinition).pedido;

if (!pedidoProto || !pedidoProto.PedidoService) {
  console.error('Error: No se pudo cargar PedidoService desde el archivo .proto');
  process.exit(1);
}

// Configurar Kafka
const kafka = new Kafka({
  brokers: ['localhost:9092'],
  createPartitioner: Partitioners.LegacyPartitioner // Usar particionador legado para evitar advertencias
});
const producer = kafka.producer();

// Crear el servidor gRPC
const server = new grpc.Server();

// Implementar el método RealizarPedido
server.addService(pedidoProto.PedidoService.service, {
  RealizarPedido: async (call, callback) => {
    const pedido = call.request;

    console.log("Pedido recibido:", pedido);

    // Publicar el pedido en Kafka
    await producer.connect();
    await producer.send({
      topic: 'pedidos',
      messages: [{ value: JSON.stringify(pedido) }],
    });
    await producer.disconnect();

    // Responder al cliente
    callback(null, { mensaje: 'Pedido procesado correctamente', exito: true });
  }
});

// Iniciar el servidor gRPC en el puerto 50051
server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {
  console.log('Servidor gRPC ejecutándose en el puerto 50051');
});
