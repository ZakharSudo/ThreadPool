# ThreadPool
Курсовая работа по многопоточному и асинхронному программированию. Часть 1

## 📋 Описание проекта

**CustomThreadPool** — это высокопроизводительная реализация пула потоков на Java с собственным механизмом балансировки задач, гибкой настройкой и расширенным логированием. Разработан для сценариев, где требуется тонкий контроль над распределением задач между потоками и очевидное логирование всех событий.

### Ключевые особенности

-  **Многоканальная балансировка** — задачи распределяются по нескольким очередям (Round Robin)
-  **Гибкая настройка** — все параметры пула конфигурируются через Builder
-  **4 политики отказа** — ABORT, CALLER_RUNS, DISCARD, DISCARD_OLDEST
-  **minSpareThreads** — гарантированное наличие свободных потоков
-  **Кастомная ThreadFactory** — логирование создания/завершения потоков
-  **Полное логирование** — все события системы фиксируются
-  **Поддержка Future** — работа с Callable задачами
-  **Graceful Shutdown** — корректное завершение работы

### Жизненный цикл потока
Создание → Ожидание задач → Получение задачи → Выполнение → Завершение
↓ ↓ ↓
Логирование Poll с таймаутом Логирование
↓
Idle timeout (если > core)
↓
Завершение


##  Быстрый старт
### Установка

bash
# Клонирование (или просто скопировать файлы)
git clone https://github.com/ZakharSudo/ThreadPool.git
cd CustomThreadPool 

# Компиляция
javac CustomThreadPool.java Main.java

# Запуск демонстрации
java Main


Параметры настройки
Подробное описание параметров
Параметр	Тип	Описание	Рекомендации
corePoolSize	int	Минимальное количество потоков	= N_CPU для CPU-bound, = N_CPU для IO-bound
maxPoolSize	int	Максимальное количество потоков	= N_CPU для CPU-bound, = N_CPU*2 для IO-bound
keepAliveTime	long	Время простоя перед завершением	30-60 сек для большинства сценариев
timeUnit	TimeUnit	Единица измерения времени	TimeUnit.SECONDS или TimeUnit.MILLISECONDS
queueSize	int	Размер очереди на поток	100-500 для средних нагрузок
minSpareThreads	int	Минимальное число свободных потоков	1-2 для предотвращения холодного старта
rejectedPolicy	RejectedPolicy	Политика при переполнении	CALLER_RUNS для надёжности

Примеры конфигураций
1. Для CPU-интенсивных задач (вычисления)
java
CustomThreadPool cpuPool = new CustomThreadPool.Builder()
    .corePoolSize(Runtime.getRuntime().availableProcessors())
    .maxPoolSize(Runtime.getRuntime().availableProcessors())
    .queueSize(200)
    .keepAliveTime(60, TimeUnit.SECONDS)
    .minSpareThreads(0)
    .rejectedPolicy(CustomThreadPool.RejectedPolicy.ABORT)
    .build();

2. Для IO-интенсивных задач (сеть, диск)
java
CustomThreadPool ioPool = new CustomThreadPool.Builder()
    .corePoolSize(Runtime.getRuntime().availableProcessors())
    .maxPoolSize(Runtime.getRuntime().availableProcessors() * 4)
    .queueSize(1000)
    .keepAliveTime(30, TimeUnit.SECONDS)
    .minSpareThreads(2)
    .rejectedPolicy(CustomThreadPool.RejectedPolicy.CALLER_RUNS)
    .build();
3. Для веб-сервера (смешанная нагрузка)
java
CustomThreadPool webPool = new CustomThreadPool.Builder()
    .corePoolSize(4)
    .maxPoolSize(16)
    .queueSize(500)
    .keepAliveTime(45, TimeUnit.SECONDS)
    .minSpareThreads(4)
    .rejectedPolicy(CustomThreadPool.RejectedPolicy.CALLER_RUNS)
    .build();


Преимущества
Низкая contention — каждая очередь обслуживается своим потоком

Равномерное распределение — задачи распределяются равномерно

O(1) сложность — выбор очереди выполняется мгновенно

Отсутствие блокировок — атомарный счётчик вместо синхронизации

Пример распределения
java
// 6 задач при corePoolSize=2
Task 1 → Queue #0 → Worker-1
Task 2 → Queue #1 → Worker-2
Task 3 → Queue #0 → Worker-1
Task 4 → Queue #1 → Worker-2
Task 5 → Queue #0 → Worker-1
Task 6 → Queue #1 → Worker-2

Обработка переполнения очереди
Если очередь переполнена:
Пробуем добавить задачу в следующую очередь (по кругу)
Если все очереди заполнены — создаём новый поток (до maxPoolSize)
Если maxPoolSize достигнут — срабатывает политика отказа

Политики отказа
Сравнение политик

Политика	    Поведение	                        Когда использовать	                Риски
ABORT	        Бросает RejectedExecutionException	Критичные задачи, нельзя потерять	Задача не выполнится
CALLER_RUNS	    Выполняет в потоке caller'а        	Надёжность важнее скорости	        Замедление caller'а
DISCARD	Молча   игнорирует задачу	                Неважные задачи, метрики	        Потеря данных
DISCARD_OLDEST	Удаляет старейшую задачу	        Real-time системы	                Потеря старых данных



Анализ производительности
Сравнение с ThreadPoolExecutor
Характеристика	    ThreadPoolExecutor	  CustomThreadPool

Балансировка	    Одна очередь	      Множество очередей + RR
Contention	        Высокий	              Низкий
minSpareThreads	    -	                  +
Гибкость настройки	Средняя	              Высокая
Логирование	        Стандартное	          Расширенное
Политики отказа	    4 стандартные	      4 кастомные
Поддержка Scheduled	+	                  -
Work Stealing	    -	                  -

