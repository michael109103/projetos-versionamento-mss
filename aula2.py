from queue import Queue, Empty
import time

# Configuração da fila principal e DLQ
main_queue = Queue()
dead_letter_queue = Queue()


def process_message(message):
    # Função que tenta processar a mensagem
    try:
        # Simulação de uma entrega de mensagem que pode falhar
        if "fail" in message:
            raise Exception("Erro na entrega da mensagem")
        print(f"Mensagem entregue com sucesso: {message}")
        return True
    except Exception as e:
        print(f"Falha na entrega da mensagem: {message}. Erro: {e}")
        return False

def handle_message_delivery(message, max_attempts):
    if "attempts" not in message:
        message["attempts"] = 0

    message["attempts"] += 1
    if process_message(message["content"]):
        return "Mensagem entregue com sucesso"
    elif message["attempts"] >= max_attempts:
        dead_letter_queue.put(message)
        return "Mensagem movida para a DLQ"
    else:
        main_queue.put(message)
        return "Mensagem reenfileirada para nova tentativa"

# Exemplo de uso:
max_attempts = 3
messages = [
    {"content": "Mensagem 1"},
    {"content": "Mensagem 2", "fail": True},
    {"content": "Mensagem 3", "fail": True},
    {"content": "Mensagem 4"},
]

for msg in messages:
    main_queue.put(msg)

while not main_queue.empty():
    message = main_queue.get()
    result = handle_message_delivery(message, max_attempts)
    print(result)

#Manutencao da DLQ e Monitoramento:
def monitor_dlq():
    while True:
        try:
            message = dead_letter_queue.get_nowait()
            print(f"Analisando mensagem na DLQ: {message}")
            # Implementar lógica de análise e possível reprocessamento
        except Empty:
            break

# Simulação de um processo de monitoramento periódico
monitor_dlq()

# Teste o processo de entrega e movimentação de mensagens para a DLQ
while not main_queue.empty():
    message = main_queue.get()
    result = handle_message_delivery(message, max_attempts)
    print(result)

# Teste o monitoramento da DLQ
monitor_dlq()

