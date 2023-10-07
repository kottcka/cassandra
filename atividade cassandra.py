from cassandra.cluster import Cluster

class ListaDeTarefasCassandra:

    def __init__(self):
        self.cluster = Cluster(['localhost'])
        self.session = self.cluster.connect('lista_tarefas')
        self.tabela = 'tasks'
        self.proximo_id = 1  # Inicialize o próximo ID como 1

        # Crie a tabela de tarefas se ela ainda não existir
        self.session.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.tabela} (
                id int PRIMARY KEY,
                title text,
                description text
            )
        """)

    def obter_proximo_id(self):
        # Obtém o próximo número sequencial disponível
        return self.proximo_id

    def adicionar_tarefa(self, titulo, descricao):
        id_tarefa = self.obter_proximo_id()
        query = f"""
            INSERT INTO {self.tabela} (id, title, description)
            VALUES (%s, %s, %s)
        """
        self.session.execute(query, (id_tarefa, titulo, descricao))
        print(f"Tarefa adicionada com ID: {id_tarefa}")

        # Incrementa o próximo ID
        self.proximo_id += 1

    def listar_tarefas(self):
        query = f"SELECT id, title FROM {self.tabela}"
        result = self.session.execute(query)
        for row in result:
            print(f"ID: {row.id}, Título: {row.title}")

    def remover_tarefa(self, tarefa_id):
        query = f"DELETE FROM {self.tabela} WHERE id = %s"
        self.session.execute(query, (tarefa_id,))
        print(f"Tarefa com ID {tarefa_id} foi removida.")

    def menu_principal(self):
        while True:
            print("\n----- Lista de Tarefas -----")
            print("1. Adicionar Tarefa")
            print("2. Listar Tarefas")
            print("3. Remover Tarefa")
            print("4. Sair")

            opcao = input("\nEscolha uma opção: ")

            if opcao == '1':
                titulo = input("Digite o título da tarefa: ")
                descricao = input("Digite a descrição da tarefa: ")
                self.adicionar_tarefa(titulo, descricao)
            elif opcao == '2':
                self.listar_tarefas()
            elif opcao == '3':
                tarefa_id = int(input("Digite o ID da tarefa que deseja remover: "))
                self.remover_tarefa(tarefa_id)
            elif opcao == '4':
                print("Saindo...")
                break
            else:
                print("\nOpção inválida! Tente novamente.")

if __name__ == "__main__":
    lista_tarefas_cassandra = ListaDeTarefasCassandra()
    lista_tarefas_cassandra.menu_principal()
1
