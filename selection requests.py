import logging
from neo4j import GraphDatabase

# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("queries.log", encoding="utf-8"), logging.StreamHandler()])

# Настройка подключения к Neo4j
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "12345678"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

# Запросы Cypher
QUERIES = {
    "1": {
        "description": "Подсчитать общее количество пользователей",
        "query": "MATCH (u:User) RETURN COUNT(u) AS total_users"
    },
    "2": {
        "description": "Подсчитать общее количество групп",
        "query": "MATCH (g:Group) RETURN COUNT(g) AS total_groups"
    },
    "3": {
        "description": "Топ-5 пользователей по количеству фолловеров",
        "query": """
            MATCH (u:User)<-[:FOLLOW]-(follower)
            RETURN u.name AS user, COUNT(follower) AS followers_count
            ORDER BY followers_count DESC
            LIMIT 5
        """
    },
    "4": {
        "description": "Топ-5 самых популярных групп",
        "query": """
            MATCH (g:Group)<-[:SUBSCRIBE]-(user)
            RETURN g.name AS group, COUNT(user) AS subscribers_count
            ORDER BY subscribers_count DESC
            LIMIT 5
        """
    },
    "5": {
        "description": "Найти пользователей, которые фолловят друг друга",
        "query": """
            MATCH (u1:User)-[:FOLLOW]->(u2:User), (u2)-[:FOLLOW]->(u1)
            RETURN u1.name AS user1, u2.name AS user2
        """
    }
}


# Функция для выполнения запросов Cypher
def execute_query(query):
    try:
        with driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]
    except Exception as e:
        logging.error(f"Ошибка выполнения запроса: {e}")
        return []


# Функция для выбора и выполнения запроса
def execute_console_query():
    print("\nВыберите запрос для выполнения:")
    for key, value in QUERIES.items():
        print(f"{key}. {value['description']}")

    # Пользователь выбирает запрос
    choice = input("\nВведите номер запроса: ")
    if choice not in QUERIES:
        print("Некорректный выбор. Попробуйте снова.")
        return

    query_info = QUERIES[choice]
    query = query_info["query"]

    print(f"\nВыполняется запрос: {query_info['description']}")
    logging.info(f"Выполняется запрос: {query_info['description']}")
    results = execute_query(query)

    if results:
        print(f"\nРезультаты запроса {query_info['description']}:")
        for result in results:
            print(result)
    else:
        print("\nРезультаты отсутствуют.")
        logging.info(f"Результаты для запроса {query_info['description']} отсутствуют.")


# Главная функция
def main():
    while True:
        execute_console_query()
        another_query = input("\nХотите выполнить еще один запрос? (да/нет): ").strip().lower()
        if another_query != "да":
            print("Программа завершена.")
            break


if __name__ == "__main__":
    main()
