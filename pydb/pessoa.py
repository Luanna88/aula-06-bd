import psycopg2 as bd
import csv
# Criar classe de configuração e método de inicialização
class Config:
    def __init__(self):
        self.config = {
            "postgres": {
                "user": "postgres",
                "password": "flor@2024",
                "host": "127.0.0.1",
                "port": "5432",
                "database": "pydb"
            }
        }
# Criar uma classe conexão para receber uma herança de Config
class Connection(Config):
    def __init__(self):
        # Chama o construtor da classe base usando super()
        super().__init__()
        try:
            # Cria a conexão e o cursor
            self.conn = bd.connect(**self.config["postgres"])
            self.cur = self.conn.cursor()
        except Exception as e:
            print("Erro na conexão", e)
            exit(1)
# Métodos de retorno à própria classe
    def __enter__(self):
        return self

    # Toda vez que sair da classe, deverá sair da conexão    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        self.connection.close()

    # Propriedades de conexão e cursor
    @property
    def connection(self):
        return self.conn

    @property
    def cursor(self):
        return self.cur

    def commit(self):
        self.connection.commit()

# Fetchall retornará em determinados registros que será passado 
    # instrução SQL
    def fetchall(self):
        return self.cursor.fetchall()

    # Método receberá a instrução SQL e passará parâmetros ou tuplas vazias
    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    # Cria uma instrução por meio de uma consulta (query)
    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()

# Criar a Classe Pessoa
class Pessoa(Connection):
    def __init__(self):
        # Chama o construtor da classe base usando super()
        super().__init__()

    # Métodos de manipulação de registros
    def insert(self, *args):
        try:
            sql = "INSERT INTO pessoa (nome) VALUES (%s)"
            # Usa o método execute da classe base
            self.execute(sql, args)
            self.commit()
            print("Registro Inserido")
        except Exception as e:
            print("Erro ao inserir", e)

# Método inserir_csv
    def insert_csv(self, filename):
        try:
            data = csv.DictReader(open(filename, encoding="utf-8"))
            for row in data:
                self.insert(row["nome"])
            print("Registros Inseridos")
        except Exception as e:
            print("Erro ao inserir csv", e)


    # Método delete
    def delete(self, id):
        try:
            sql_s = f"SELECT * FROM pessoa WHERE id = {id}"
            if not self.query(sql_s):
                print("Registro não encontrado para deletar")
                return
            sql_d = f"DELETE FROM pessoa WHERE id = {id}"
            self.execute(sql_d)
            self.commit()
            print("Registro deletado")
        except Exception as e:
            print("Erro ao deletar", e)

    # Método update
    def update(self, id, *args):
        try:
            sql = f"UPDATE pessoa SET nome = %s WHERE id = {id}"
            self.execute(sql, args)
            self.commit()
            print("Registro atualizado")
        except Exception as e:
            print("Erro ao atualizar", e)



#testar instancias
if __name__ == "__main__":
    pessoa = Pessoa()
    pessoa.insert("João")
    pessoa.insert_csv("data.csv")
    pessoa.insert("Maria")
    pessoa.insert_csv("data.csv")
    pessoa.insert("Miguel")
    pessoa.insert_csv("data.csv")
    pessoa.insert("Ana")
    pessoa.insert_csv("data.csv")
    pessoa.insert("Carlos")
    pessoa.insert_csv("data.csv")
    pessoa.insert("Isabel")
    pessoa.insert_csv("data.csv")
    pessoa.delete(3)
    pessoa.update(2, "Maria Antonio")
    
