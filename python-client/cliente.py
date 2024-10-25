import grpc
import pedido_pb2
import pedido_pb2_grpc
import csv
import os

def run():
    # Obtener la ruta absoluta del archivo dataset_sales.csv
    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join(current_dir, 'dataset_sales.csv')

    # Crear un canal gRPC al servidor de gestión de pedidos
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = pedido_pb2_grpc.PedidoServiceStub(channel)

        # Leer el dataset de compras (dataset_sales.csv)
        with open(csv_file_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Crear un objeto de pedido a partir de cada fila
                compra = pedido_pb2.PedidoRequest(
                    email_cliente=row['Email_Client'],
                    producto=row['Product'],
                    precio=float(row['Price']),
                    pasarela_pago=row['Pay_Method'],
                    banco=row['Bank'],
                    marca_tarjeta=row['Type_Card'],
                    direccion=row['Street'],
                    numero=row['Number'],
                    region=row['Region']
                )

                # Enviar el pedido al servidor gRPC
                response = stub.RealizarPedido(compra)
                print(f"Compra enviada: {response.mensaje}, Éxito: {response.exito}")

if __name__ == '__main__':
    run()
