const { Kafka } = require('kafkajs');

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'gestion-estados-group' });
const producer = kafka.producer();

const estados = ["Procesando", "Preparación", "Enviado", "Entregado"];

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'pedidos', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const pedido = JSON.parse(message.value.toString());

      // Actualizar estados y reenviar a Kafka
      for (const estado of estados) {
        await new Promise(resolve => setTimeout(resolve, 2000)); // Simulación de tiempo
        pedido.estado = estado;
        await producer.send({
          topic: 'estado_pedidos',
          messages: [{ value: JSON.stringify(pedido) }],
        });
        console.log(`Pedido actualizado: ${pedido.producto}, Estado: ${estado}`);
      }
    },
  });
};

run().catch(console.error);

//guardar timestamp para ver metricas