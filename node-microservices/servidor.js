const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { Kafka } = require('kafkajs');

const PROTO_PATH = './proto/pedido.proto';

// Cargar el archivo .proto
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const pedidoProto = grpc.loadPackageDefinition(packageDefinition).pedido;

// Verificar si `pedidoProto` contiene el servicio `PedidoService`
console.log(pedidoProto);  // Esto te mostrará si `PedidoService` está cargado correctamente

const kafka = new Kafka({ brokers: ['localhost:9092'] }); //broker: servidor de kafka 
const producer = kafka.producer(); 

const server = new grpc.Server();

if (pedidoProto && pedidoProto.PedidoService) {
  server.addService(pedidoProto.PedidoService.service, {
    RealizarPedido: async (call, callback) => {
      const pedido = call.request;
      
      // Publicar el pedido en Kafka
      await producer.connect();
      await producer.send({
        topic: 'pedidos',
        messages: [{ value: JSON.stringify(pedido) }],
      });
      await producer.disconnect();

      callback(null, { mensaje: 'Pedido procesado', exito: true });
    },
  });

  server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), () => {
    console.log('Servidor gRPC ejecutándose en el puerto 50051');
    server.start();
  });
} else {
  console.error('Error: No se pudo cargar PedidoService desde el archivo .proto');
  console.log(pedidoProto);
}
