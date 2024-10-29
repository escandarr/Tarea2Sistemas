import grpc
import os
import csv
import time
from pedido_pb2 import PedidoRequest
from pedido_pb2_grpc import PedidoServiceStub

def realizar_pedido(stub, producto, precio, email, metodo_pago, banco, tipo_tarjeta, direccion, numero, region):
    pedido = PedidoRequest(
        email_cliente=email,
        producto=producto,
        precio=precio,
        pasarela_pago=metodo_pago,
        banco=banco,
        marca_tarjeta=tipo_tarjeta,
        direccion=direccion,
        numero=numero,
        region=region
    )

    respuesta = stub.RealizarPedido(pedido)
    print(f"Respuesta del servidor: {respuesta.mensaje}, Éxito: {respuesta.exito}")

def procesar_dataset(stub, ruta_dataset):
    with open(ruta_dataset, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            realizar_pedido(
                stub,
                row['Product'],
                float(row['Price']),
                row['Email_Client'],
                row['Pay_Method'],
                row['Bank'],
                row['Type_Card'],
                row['Street'],
                row['Number'],
                row['Region']
            )
            time.sleep(2)

def conectar_servidor():
    canal = grpc.insecure_channel('localhost:50052')
    stub = PedidoServiceStub(canal)
    print("Conectado al servidor gRPC en localhost:50052")
    return stub

if __name__ == "__main__":
    stub = conectar_servidor()
    ruta_dataset = os.path.join(os.path.dirname(__file__), 'dataset_sales.csv')
    procesar_dataset(stub, ruta_dataset)
