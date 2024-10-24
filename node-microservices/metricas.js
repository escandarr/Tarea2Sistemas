const { Kafka } = require('kafkajs');
const { Client } = require('@elastic/elasticsearch');

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'metricas-group' });
const elasticClient = new Client({ node: 'http://localhost:9200' });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'estado_pedidos', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const pedido = JSON.parse(message.value.toString());

      // Almacenar en ElasticSearch
      await elasticClient.index({
        index: 'pedidos',
        body: pedido,
      });

      console.log(`Pedido almacenado en ElasticSearch: ${pedido.producto}, Estado: ${pedido.estado}`);
    },
  });
};

run().catch(console.error);
