from datetime import datetime

class Event:
    def __init__(self, event_type, amount, timestamp=None, user_id=None):
        self.event_type = event_type
        self.amount = amount
        self.timestamp = timestamp if timestamp else datetime.utcnow()
        self.user_id = user_id

class BankAccount:
    def __init__(self):
        self.events = []
        self.balance = 0

    def apply_event(self, event):
        if event.event_type == "deposit":
            self.balance += event.amount
        elif event.event_type == "withdrawal":
            self.balance -= event.amount

    def add_event(self, event):
        self.events.append(event)
        self.apply_event(event)

    def get_balance(self):
        return self.balance

    def reconstruct_state(self):
        self.balance = 0
        for event in self.events:
            self.apply_event(event)

# Implementação da Lógica de Eventos:

def deposit(account, amount, user_id=None):
    event = Event(event_type="deposit", amount=amount, user_id=user_id)
    account.add_event(event)

def withdraw(account, amount, user_id=None):
    event = Event(event_type="withdrawal", amount=amount, user_id=user_id)
    account.add_event(event)

# Aplicação dos Eventos ao Estado:
# O método reconstruct_state na classe BankAccount processa todos os eventos para atualizar o saldo.
# Testes:
# Criação de uma nova conta bancária
account = BankAccount()

# Realização de depósitos e retiradas
deposit(account, 100, user_id=1)
withdraw(account, 50, user_id=1)
deposit(account, 200, user_id=1)

# Verificação do saldo atual
print(f"Saldo atual: {account.get_balance()}")  # Esperado: 250

# Reconstrução do estado da conta
account.reconstruct_state()
print(f"Saldo após reconstrução: {account.get_balance()}")  # Esperado: 250

# Monitoramento e Auditoria:
def print_event_history(account):
    for event in account.events:
        print(f"Tipo: {event.event_type}, Montante: {event.amount}, Data: {event.timestamp}, Usuário: {event.user_id}")

# Exemplo de uso:
print_event_history(account)

