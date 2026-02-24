## Ссылки

### Docker compose

https://medium.com/@dkalouris/setting-up-spark-using-docker-59db2d073487
https://medium.com/@dkalouris/setting-up-and-connecting-airflow-and-spark-using-docker-compose-9773dec21bc8

### Spark Cross Join

https://towardsdev.com/spark-beyond-basics-cross-joins-in-spark-5d876945ec9b

### Геолокация

https://www.findlatitudeandlongitude.com/


## Запуск

### Запуск контейнеров

docker compose up

### Компиляция приложения

docker exec -it spark-master /bin/bash
root@PID:/opt/spark# cd /app
root@PID:/app# sbt assembly