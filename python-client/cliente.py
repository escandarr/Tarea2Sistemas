import csv
import grpc
import time
import os
from pedido_pb2 import PedidoRequest  # Importa la clase PedidoRequest
from pedido_pb2_grpc import PedidoServiceStub  # Importa el servicio PedidoServiceStub

def realizar_pedido(stub, nombre_producto, precio, cliente_email, metodo_pago, banco, tipo_tarjeta, calle, numero, region):
    """
    Función para crear una solicitud de pedido y enviarla al servidor gRPC.
    """
    # Crear la solicitud de pedido para el Servidor gRPC
    pedido = PedidoRequest(
        producto=nombre_producto,
        precio=precio,
        email_cliente=cliente_email,
        pasarela_pago=metodo_pago,
        banco=banco,
        marca_tarjeta=tipo_tarjeta,
        direccion=calle,
        numero=numero,
        region=region
    )

    # Enviar el pedido al Servidor gRPC
    respuesta = stub.RealizarPedido(pedido)
    print(f"Respuesta del servidor: {respuesta.mensaje}, Éxito: {respuesta.exito}")

def procesar_dataset(ruta_dataset, stub):
    """
    Función para leer el archivo CSV y procesar cada fila como un pedido.
    """
    with open(ruta_dataset, 'r') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            # Extraer los datos de la fila actual
            nombre_producto = row['Product']
            precio = float(row['Price'])
            cliente_email = row['Email_Client']
            metodo_pago = row['Pay_Method']
            banco = row['Bank']
            tipo_tarjeta = row['Type_Card']
            calle = row['Street']
            numero = row['Number']
            region = row['Region']

            # Enviar el pedido al servidor
            realizar_pedido(stub, nombre_producto, precio, cliente_email, metodo_pago, banco, tipo_tarjeta, calle, numero, region)

            # Esperar 2 segundos entre cada pedido para simular una pausa
            time.sleep(2)

def conectar_servidor():
    """
    Función para establecer la conexión con el servidor gRPC y devolver el stub.
    """
    # Conectar al Servidor gRPC
    canal_servidor = grpc.insecure_channel('localhost:50051')  # Asegúrate de usar el puerto correcto
    stub = PedidoServiceStub(canal_servidor)
    print("Conectado al servidor gRPC en localhost:50051")
    return stub

if __name__ == "__main__":
    # Conectar al servidor gRPC
    stub = conectar_servidor()

    # Construir la ruta absoluta del dataset
    ruta_dataset = os.path.join(os.path.dirname(__file__), 'dataset_sales.csv')

    # Procesar el dataset
    procesar_dataset(ruta_dataset, stub)
