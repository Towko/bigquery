-- Завдання 1. Напиши SQL-запит, який:
-- 1. Створює тимчасову таблицю temp_logs, у якій:
-- - поле user_id містить ідентифікатор користувача;
-- - поле details — це STRUCT, що містить два вкладені поля:
-- event — назва події (наприклад 'search'),
-- screen — назва екрана (наприклад 'home').
-- 2. Виводить окремо вкладені поля details.event та details.screen.
-- Підказка 
-- Після створення таблиці виконай окремий запит, де покажеш user_id, details.event. details.screen

-- Завдання 1
create temp table temp_logs as -- ствоюємо тимчасову таблицю
select 1 as user_id, [
  struct('search' as event, 'home' as screen)
] as details -- робимо в ній масив з даними
;

select event, screen
from temp_logs
cross join unnest(details) -- виводимо окремо вкладені поля details.event та details.screen
;


-- Завдання 2. Порахуй кількість унікальних користувачів, які здійснили подію purchase за останні 3 дні включно з сьогодні. Використай фільтр по _PARTITIONDATE
select
count (distinct user_id) as unique_users -- рахуємо унікальних користувачів
from goit-homework-478807.homework_bigquery.event_logs
where event_name = 'purchase' 
  and _PARTITIONDATE between '2025-10-18' and current_date() -- виконуємо умови відображення
;

-- Завдання 3. Побудуй масив дій користувача через ARRAY_AGG() у межах дня. 
-- Згрупуй усі події (event) кожного користувача за датою (DATE(event_time)) і збери їх у масив.
select 
  user_id, 
  array_agg(struct(event_name, event_time)) as behaviour -- створюємо масив дій користувача 
from goit-homework-478807.homework_bigquery.event_logs
group by user_id, date(event_time) -- групуємо по даті
;


-- Завдання 4. Створи STRUCT (вручну або на базі наявного поля) і витягни вкладене туди поле в окремому запиті або СТЕ. 
-- Виведи user_id as uid, screen as location.

with struct_cte as -- створюємо CTE функцію з масивом
(
  select 
    user_id,
    array_agg(struct(event_name, event_time, screen)) as structure,
  from self-homework.self_homework_data.event_logs
  group by user_id
)
select user_id as uid, screen as location
from struct_cte
cross join unnest(structure) -- розкладаємо масив і виводимо з нього дані в стовпчики uid та location
;

-- Завдання 5. Налаштуй запланований запит (Scheduled Query), 
-- який щодня рахує кількість унікальних покупців з '2024-01-01’ (включно) та зберігає результат у нову таблицю.

create or replace table `goit-homework-478807.homework_bigquery.daily_unique_purchasers` as  -- перший запит - створюємо таблицю
select
  date(event_time) as event_date,
  count(distinct user_id) as unique_users -- знаходимо унікальних покупців
from `goit-homework-478807.homework_bigquery.event_logs`
where event_name = 'purchase' -- перевіряємо за фільтром "покупка" і "дата"
  and date(event_time) >= '2024-01-01'
group by event_date -- групуємо і сортуємо за датою
order by event_date
;

insert into `goit-homework-478807.homework_bigquery.daily_unique_purchasers` -- додаємо до існуючої таблиці нові дані за поточний день
select
  date(event_time) as event_date,
  count(distinct user_id) as unique_users
from `goit-homework-478807.homework_bigquery.event_logs`
where event_name = 'purchase'
  and date(event_time) = current_date() -- знаходимо унікальних покупців за поточний день
group by event_date
;


-- Завдання 7. Розгорни масив із Завдання 3 за допомогою UNNEST() і виведи всі унікальні події користувача за день.
with aggregate_cte as --розміщуємо наш масив в CTE запиті
(
  select 
    user_id, 
    array_agg(struct(event_name, event_time)) as behaviour
  from goit-homework-478807.homework_bigquery.event_logs
  group by user_id, date(event_time)
)
 
select user_id, 
  string_agg(distinct(event_name)) as events  -- для зручної візуалізації об'єднуємо опис дій кожного користувача в рядок
from aggregate_cte  -- беремо масив з CTE запиту
cross join unnest(behaviour) -- розкладаємо масив
group by user_id
;