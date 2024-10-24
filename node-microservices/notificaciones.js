const { Kafka } = require('kafkajs');
const nodemailer = require('nodemailer');

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const consumer = kafka.consumer({ groupId: 'notificaciones-group' });

const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: 'tu_correo@gmail.com',
    pass: 'tu_contraseña',
  },
});

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'estado_pedidos', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const pedido = JSON.parse(message.value.toString());

      const mailOptions = {
        from: 'tienda@ejemplo.com',
        to: pedido.email_cliente,
        subject: 'Actualización de Pedido',
        text: `Tu pedido de ${pedido.producto} está ahora en estado: ${pedido.estado}.`,
      };

      transporter.sendMail(mailOptions, (error, info) => {
        if (error) console.log(error);
        console.log(`Email enviado a ${pedido.email_cliente} sobre el estado ${pedido.estado}`);
      });
    },
  });
};

run().catch(console.error);
