import gspread
import time
import json
import socket
from datetime import datetime
from google.oauth2.service_account import Credentials

# Configutação do socket client dos pedidos
DESTINATION_ADDR = '0.0.0.0'
SOURCE_PORT, DESTINATION_PORT = 9090, 8089

orders_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
orders_client_socket.bind(('localhost',SOURCE_PORT))
orders_client_socket.connect((DESTINATION_ADDR, DESTINATION_PORT))

# Configuração das credenciais e inicialização do cliente gspread
scopes = [
    "https://www.googleapis.com/auth/spreadsheets"
]
creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
client = gspread.authorize(creds)

# Acessa a planilha e a worksheet específica
sheet_id = "1Kw-RtdJLl-Tk8D5OdWT1vCTw9eyZEQcTHjg-4z5R0-M"
orders_sheet = client.open_by_key(sheet_id)
worksheet = orders_sheet.worksheet("Pedidos")

# Colore a linha do pedido
def color_cells(row_index, status):
    if status:
        worksheet.format(f'A{row_index}:C{row_index}', {
            'backgroundColor':{
                'red': 0.8,
                'green': 1.0,
                'blue': 0.8
            }
        })
    else:    
        worksheet.format(f'A{row_index}:C{row_index}', {
            'backgroundColor':{
                'red': 1.0,
                'green': 0.8,
                'blue': 0.8
            }
        })
        
# Transforma array em objeto json    
def data_transform(orders_info):
    # Adiciona a data do pedido
    date = datetime.now().strftime("%d/%m/%Y")
    
    order_dict = {
        'date': date,
        'product_num': orders_info[0],
        'qty': int(orders_info[1]),
        'order_num': int(orders_info[2]),
    }
    order_json = json.dumps(order_dict)
    print(order_json)
    return order_json
    
# Verifica a planilha de pedidos a cada 1 segundo 
def watch_changes():
    # Pega os dados iniciais e cria um dicionário para rastrear mudanças
    previous_data = {i: row for i, row in enumerate(worksheet.get("C4:C50"), start=4)}
    
    while True:
        # Obtém o estado atual das células do intervalo
        current_data = {i: row for i, row in enumerate(worksheet.get("C4:C50"), start=4)}
        
        # Itera sobre cada linha para detectar alterações
        for row_index in current_data:
            prev = previous_data.get(row_index)
            curr = current_data.get(row_index)
            
            if prev != curr:
                # Se houver mudança, imprime e aplica destaque
                new_row_data = worksheet.row_values(row_index)
                print(f"MUDOU na linha {row_index}:", new_row_data)
                order_json = data_transform(new_row_data)
                orders_client_socket.send(bytes(order_json,'UTF-8'))
                
                 # Aguarda resposta do servidor para determinar o status
                server_response = orders_client_socket.recv(1024).decode('UTF-8')
                response_data = json.loads(server_response)
                status = response_data.get('status', 'pending')
                message = response_data.get('message','')
                
                color_cells(row_index, status)
        
        # Atualiza `previous_data` com o estado atual completo
        previous_data = current_data.copy()
        
        # Pausa antes da próxima verificação
        time.sleep(1)

watch_changes()