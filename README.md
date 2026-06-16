# Как запускать

1. Запустить docker:

`$ docker compose up -d`

Создастся папка runtime с данными PostgreSQL.

2. Инициализировать PostgreSQL:

```
$ cd sql/
$ sh populate.sh
```

3. Запустить producer и consumer в двух разных консолях *(не забыть предварительно создать virtualenv)*
```
$ python3 kafka_producer.py
$ python3 kafka_consumer.py 
```

4. Запустить java-проект (запускать лучше в IDEA, как запускать просто с maven не разобрался).
