import requests
import time
import logging
from neo4j import GraphDatabase

# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("app.log", encoding="utf-8"), logging.StreamHandler()])

# Конфигурация VK API
ACCESS_TOKEN = 'fbc4d15dfbc4d15dfbc4d15d90f8e7b200ffbc4fbc4d15d9cf4e4335ed2494b10195ea5'
USER_ID = '190868'
VERSION = '5.131'

# Настройка подключения к Neo4j
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "12345678"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

# Словарь для отслеживания обработанных пользователей
processed_users = set()


# Функция для получения информации о пользователе
def get_user_info(user_id):
    if user_id in processed_users:
        return None  # Пользователь уже обработан

    try:
        response = requests.get(
            'https://api.vk.com/method/users.get',
            params={
                'user_ids': user_id,
                'fields': 'followers_count,city,sex',
                'access_token': ACCESS_TOKEN,
                'v': VERSION
            }
        )
        data = response.json()
        if 'response' in data:
            user = data['response'][0]
            user_info = {
                'id': user['id'],
                'full_name': f"{user['first_name']} {user['last_name']}",
                'followers_count': user.get('followers_count', 0),
                'sex': user.get('sex', 0),
                'home_town': user.get('city', {}).get('title', '')
            }
            processed_users.add(user_id)
            logging.info(f"Получена информация о пользователе {user_info['full_name']} (ID: {user_info['id']})")
            return user_info
        else:
            logging.error(f"Ошибка получения информации о пользователе: {data['error']['error_msg']}")
    except Exception as e:
        logging.error(f"Ошибка: {e}")
    return None


# Функция для получения фолловеров
def get_followers(user_id, limit=None):
    try:
        response = requests.get(
            'https://api.vk.com/method/users.getFollowers',
            params={
                'user_id': user_id,
                'count': limit if limit else 1000,  # Ограничиваем только если указан `limit`
                'access_token': ACCESS_TOKEN,
                'v': VERSION
            }
        )
        data = response.json()
        if 'response' in data:
            followers = data['response'].get('items', [])
            logging.info(f"Найдено {len(followers)} фолловеров для пользователя {user_id}")
            return followers
        elif 'error' in data and data['error']['error_msg'] == "This profile is private":
            logging.warning(f"Профиль пользователя {user_id} приватный. Пропускаем.")
            return []
        else:
            logging.error(f"Ошибка получения фолловеров: {data['error']['error_msg']}")
    except Exception as e:
        logging.error(f"Ошибка: {e}")
    return []


# Функция для получения подписок
def get_subscriptions(user_id):
    try:
        response = requests.get(
            'https://api.vk.com/method/users.getSubscriptions',
            params={
                'user_id': user_id,
                'extended': 1,  # Расширенный запрос для получения данных о группах
                'fields': 'members_count',
                'access_token': ACCESS_TOKEN,
                'v': VERSION
            }
        )
        data = response.json()
        if 'response' in data:
            subscriptions = data['response'].get('items', [])
            logging.info(f"Найдено {len(subscriptions)} подписок для пользователя {user_id}")

            # Собираем данные о группах
            group_data = []
            for group in subscriptions:
                group_data.append({
                    'id': group['id'],
                    'name': group.get('name', ''),
                    'screen_name': group.get('screen_name', ''),
                    'members_count': group.get('members_count', 0)
                })
            return group_data
        elif 'error' in data and data['error']['error_msg'] == "This profile is private":
            logging.warning(f"Профиль пользователя {user_id} приватный. Пропускаем.")
            return []
        else:
            logging.error(f"Ошибка получения подписок: {data['error']['error_msg']}")
    except Exception as e:
        logging.error(f"Ошибка: {e}")
    return []


# Рекурсивная функция для получения данных
def get_user_data(user_id, depth, follower_limit=30):
    if depth == 0:
        return []

    user_info = get_user_info(user_id)
    if not user_info:
        return []

    # Получаем всех фолловеров и записываем в базу
    all_followers = get_followers(user_id)
    # Ограничиваем обработку только первых `follower_limit`
    limited_followers = all_followers[:follower_limit]

    subscriptions = get_subscriptions(user_id)

    followers_data = []
    for follower_id in limited_followers:
        time.sleep(0.34)  # Задержка между запросами
        follower_data = get_user_data(follower_id, depth - 1, follower_limit)
        followers_data.extend(follower_data)

    return [{
        'user_info': user_info,
        'all_followers': all_followers,  # Сохраняем всех фолловеров
        'processed_followers': limited_followers,  # Обрабатываем ограниченное число
        'subscriptions': subscriptions
    }] + followers_data


# Функция для записи данных в Neo4j
def save_to_neo4j(data):
    with driver.session() as session:
        for item in data:
            user_info = item['user_info']
            session.execute_write(create_user, user_info['id'], user_info['full_name'], user_info['sex'],
                                  user_info['home_town'])

            # Записываем всех фолловеров
            for follower_id in item['all_followers']:
                session.execute_write(create_user, follower_id, '', 0, '')
                session.execute_write(create_follow, user_info['id'], follower_id)

            # Записываем группы (подписки)
            for group in item['subscriptions']:
                session.execute_write(create_group, group['id'], group['name'], group['screen_name'],
                                      group['members_count'])
                session.execute_write(create_subscribe, user_info['id'], group['id'])


# Функции для работы с Neo4j
def create_user(tx, user_id, full_name, sex, home_town):
    tx.run("""
    MERGE (u:User {id: $user_id})
    SET u.name = $full_name, u.sex = $sex, u.home_town = $home_town
    """, user_id=user_id, full_name=full_name, sex=sex, home_town=home_town)


def create_group(tx, group_id, name, screen_name, subscribers_count):
    tx.run("""
    MERGE (g:Group {id: $group_id})
    SET g.name = $name, g.screen_name = $screen_name, g.subscribers_count = $subscribers_count
    """, group_id=group_id, name=name, screen_name=screen_name, subscribers_count=subscribers_count)


def create_follow(tx, user_id, follower_id):
    tx.run("""
    MATCH (u1:User {id: $user_id})
    MATCH (u2:User {id: $follower_id})
    MERGE (u2)-[:FOLLOW]->(u1)
    """, user_id=user_id, follower_id=follower_id)


def create_subscribe(tx, user_id, group_id):
    tx.run("""
    MATCH (u:User {id: $user_id})
    MATCH (g:Group {id: $group_id})
    MERGE (u)-[:SUBSCRIBE]->(g)
    """, user_id=user_id, group_id=group_id)


# Функция для удаления данных из базы
def delete_data():
    with driver.session() as session:
        session.execute_write(delete_all_data)


def delete_all_data(tx):
    tx.run("MATCH (n) DETACH DELETE n")


# Главная функция
def main():
    #delete_data()
    #logging.info("Все данные удалены из Neo4j")

    data = get_user_data(USER_ID, depth=3, follower_limit=50)
    save_to_neo4j(data)
    logging.info("Данные успешно сохранены в Neo4j")


if __name__ == "__main__":
    main()
