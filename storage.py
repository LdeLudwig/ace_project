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

active_connections = {}


def get_last_row():
    """ 
        Busca a última linha preechida na planilha. Se não encontrar nada, volta a primeira linha dos dados.
    """
    all_data = worksheet.get_all_values()
    for i in range(len(all_data) - 1, 3, -1):  # Itera de baixo para cima, começando na linha 4
        if any(all_data[i]):  # Se a linha não estiver vazia
            return i + 1  # Retorna índice 1-based
    return 4  # Retorna 4 se não encontrar nenhuma linha preenchida


def handle_orders(json_object, connection):
    """ 
        Recebe o pedido, verifica o estoque e devolve o status do estoque.
        O status do estoque é avaliado em uma seção de condicionamento. 
    """
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
        else:
            print(f"Erro: Estoque insuficiente para o produto {product_num}. Pedido #{order_num} recusado.")
            return False
    except Exception as e:
        print(f"Erro ao processar pedido: {str(e)}")
        return False
    return True
    
      
def sending_order_to_manipulator(json_object, stock_status, target_addr):
    """
        Verifica o estado do estoque e envia o pedido para o microcontrolador
    """
    if stock_status and target_addr in active_connections:
        target_connection = active_connections[target_addr]
        
        try:
            target_connection.send(bytes(json_object,'UTF-8'))
            print(f"Pedido enviado para {target_addr} (Manipulador)")
        except Exception as e:
            print(f"Erro ao enviar para {target_addr} (Manipulador)")
    else:
        print(f"Erro: Client {target_addr} não está conectado ou estoque insuficiente")
        

def handle_weight_changes(json_object):
    """ 
        Lida com a requisição enviada das balanças
    """
    date = json_object['date']
    operation = json_object['operation']
    product_num = json_object['product_num']
    qty_requested = json_object['qty']
    order_num = json_object['order_num']

    # Obtem a última linha preenchida do estoque
    last_row = get_last_row()
    last_data = worksheet.row_values(last_row)
    headers_op = worksheet.row_values(2) # Cabeçalhos esperados na linha 2
    headers = worksheet.row_values(4)  # Cabeçalhos esperados na linha 4
    
    # Identifica as colunas relevantes
    op_in_key = f"Entrada {product_num}"
    op_out_key = f"Saída {product_num}"
    product_in_key = f"Quant. total prod. {product_num}"
    total_key = "Saldo total prod."
    
    product_in_col = headers.index(product_in_key)
    
    op_in_col = headers_op.index(op_in_key)
    op_out_col = headers_op.index(op_out_key)

    total_col = headers.index(total_key)
    

    # Recuperar os saldos atuais
    current_stock = int(last_data[product_in_col]) if last_data[product_in_col] else 0
    current_total = int(last_data[total_col]) if last_data[total_col] else 0
    
    
    if operation == 1:
        # Calcula os novos valores
        new_stock = current_stock + qty_requested
        new_total = current_total + qty_requested
    else:
        # Calcula os novos valores
        new_stock = current_stock - qty_requested
        new_total = current_total - qty_requested
        
    # Criar nova linha replicando os valores antigos
    new_row = last_data.copy()
    new_row[0] = date  
    new_row[op_out_col] = qty_requested if operation == 0 else 0
    new_row[op_in_col] =  qty_requested if operation == 1 else 0
    new_row[product_in_col] = new_stock  
    new_row[total_col] = new_total  

    # Adicionar a nova linha à planilha
    worksheet.append_row(new_row)
    print(f"Pedido #{order_num} processado com sucesso! Estoque do produto {product_num} atualizado para {new_stock}. Saldo total: {new_total}.")


def process_connection(connection, addr):
    """ 
        Router das conexões do Socket para  
    """
    # Adiciona a conexão ao dicionário global
    active_connections[addr] = connection

    try:
        while True:
            buf = connection.recv(3000)
            if not buf:
                print(f"Conexão encerrada com {addr}")
                break
            
            # Decodifica os dados recebidos
            try:
                data = buf.decode('utf-8')
                json_object = json.loads(data)
                
                if addr[1] == 9090:
                    # Processa pedidos de cliente na porta 9090
                    stock_status = handle_orders(json_object, connection)
                    if stock_status:
                        print(f"O status é {stock_status}, ")
                    """ if stock_status:
                        # Envia para outro cliente (exemplo: escolhe o primeiro conectado, exceto o atual)
                        target_addr = next(
                            (a for a in active_connections if a != addr),
                            None
                        )
                        if target_addr:
                            print(target_addr)
                            sending_order_to_manipulator(json_object, stock_status, target_addr)
                        else:
                            print("Nenhum outro cliente conectado para envio.") """
                else:
                    handle_weight_changes(json_object)
                    print(f"Conexão recebida do microcontrolador: {addr}")
            except json.JSONDecodeError:
                print("Erro: Dados recebidos não estão no formato JSON:", buf.decode('utf-8'))
                connection.send(json.dumps({"status": False, "message": "Formato inválido"}).encode('utf-8'))
    except Exception as e:
        print(f"Erro na conexão com {addr}: {str(e)}")
    finally:
        # Remove a conexão do dicionário e fecha a conexão
        del active_connections[addr]
        connection.close()


def accept_connections():
    """ 
        Recebe conexões de cria uma thread para cada socket conectado
    """        
    while True:
        connection, addr = server_socket.accept()
        print(f"{addr} connected")
        thread = Thread(target=process_connection, args=(connection, addr))
        thread.start()


accept_connections()
