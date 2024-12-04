import gspread
import json
import socket
from threading import Thread
from google.oauth2.service_account import Credentials

# Configuração do socket server
SERVER_ADDR = '0.0.0.0'
SERVER_PORT = 8089
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.settimeout(None)
server_socket.bind((SERVER_ADDR, SERVER_PORT))
server_socket.listen(5)

# Configuração das credenciais e inicialização do cliente gspread
scopes = [
    "https://www.googleapis.com/auth/spreadsheets"
]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# Acessa a planilha e a worksheet específica
sheet_id = '10XiI62jg7ElYyf1AWhK_bp41C2zeU7-U_D-lTlQHMYY'
storage_sheet = client.open_by_key(sheet_id)
worksheet = storage_sheet.worksheet("Estoque")

def get_last_row():
    all_data = worksheet.get_all_values()
    for i in range(len(all_data) - 1, 3, -1):  # Itera de baixo para cima, começando na linha 4
        if any(all_data[i]):  # Se a linha não estiver vazia
            return i + 1  # Retorna índice 1-based
    return 4  # Retorna 4 se não encontrar nenhuma linha preenchida


def handle_orders(json_object, connection):
    try:
        date = json_object['date']
        product_num = json_object['product_num']
        qty_requested = json_object['qty']
        order_num = json_object['order_num']

        # Obtem a última linha preenchida do estoque
        last_row = get_last_row()
        last_data = worksheet.row_values(last_row)
        headers = worksheet.row_values(4)  # Cabeçalhos esperados na linha 4
        product_key = f"Quant. total prod. {product_num}"

        # Mapeia índice da coluna do produto
        if product_key in headers:
            product_col = headers.index(product_key)
        else:
            print(f"Erro: Produto '{product_num}' não encontrado na planilha.")
            connection.send(json.dumps({"status": False, "message":f"Produto {product_num} não encontrado"}).encode('utf-8'))
            return

        # Verifica saldo atual do produto
        current_stock = int(last_data[product_col]) if product_col < len(last_data) and last_data[product_col] else 0
        print(f"Estoque atual do produto {product_num}: {current_stock}")

        # Valida estoque
        if current_stock >= qty_requested:
            connection.send(json.dumps({"status": True,"message":f"Pedido {order_num} processado com sucesso!"}).encode('utf-8'))
            
            sending_order_to_manipulator(json_object=json_object)

        else:
            print(f"Erro: Estoque insuficiente para o produto {product_num}. Pedido #{order_num} recusado.")
            connection.send(json.dumps({"status": False, "message":f"Estoque insuficiente para o produto {product_num}"}).encode('utf-8'))
            return False
    except Exception as e:
        print(f"Erro ao processar pedido: {str(e)}")
        return False
    return True
    
    
    
def sending_order_to_manipulator(json_object, stock_status, connection):
        # verifica o estado do estoque e envia o pedido para o microcontrolador
    if stock_status:
        connection.send(bytes(json_object,'UTF-8'))
        """ 
        
        Adicionar lógica para envio do pedido para o robô
        
        """
    return 1

def hendle_weight_changes(json_object):
    """ 
    Implementar essa parte quando a balança registrar uma mudança de peso. 
    Inserir uma condicionamento se tirar e se colocar itens na balança. As duas situações devem alterar o estoque.
    
    
    new_stock = current_stock - qty_requested

    # Atualiza planilha
    new_row = [date] + [""] * (product_col - 1) + [new_stock] + [""] * (len(headers) - product_col - 1)
    worksheet.append_row(new_row)
    print(f"Pedido #{order_num} processado com sucesso! Estoque atualizado para {new_stock}.")            
    
    """
    return 1


def process_connection(connection, addr):
    try:
        while True:
            buf = connection.recv(3000)
            if not buf:
                print(f"Conexão encerrada com {addr}")
                break
            
            print(addr)
            if addr[1] == 9090:
                try:
                    # Decodifica os dados recebidos e converte para JSON
                    data = buf.decode('utf-8')
                    json_object = json.loads(data)
                    order = handle_orders(json_object, connection)
                    
                    if order:
                        sending_order_to_manipulator(json_object)
                except json.JSONDecodeError:
                    print("Erro: Dados recebidos não estão no formato JSON:", buf.decode('utf-8'))
                    connection.send(json.dumps({"status": False, "message": "Formato inválido"}).encode('utf-8'))
            else:
                sending_order_to_manipulator(json_object,stock_status=handle_orders, connection=connection)
                data = buf.decode('utf-8')
                print(f"Conexão recebida do microcontrolador: {addr}")
                #connection.send("Conexão aceita, mas lógica ainda não definida.".encode('utf-8'))
                
                
    except Exception as e:
        print(f"Erro na conexão com {addr}: {str(e)}")
    finally:
        connection.close()


def accept_connections():
    # Servidor esperando por conexões        
    while True:
        connection, addr = server_socket.accept()
        print(f"{addr}")
        thread = Thread(target=process_connection, args=(connection, addr))
        thread.start()


accept_connections()
