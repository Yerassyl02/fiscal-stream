# Базируемся на bitnami Spark
FROM bitnami/spark:3.5

# Переходим на root, чтобы была возможность ставить пакеты
USER root

# Устанавливаем Python 3 и pip
RUN install_packages python3 python3-pip

# (опционально) Указываем Spark использовать именно этот python
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

USER 1001