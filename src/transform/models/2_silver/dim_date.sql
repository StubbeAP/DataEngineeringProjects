with date_spine as (
    select
        dateadd(day, seq4(), '2020-01-01'::date) as date_actual
    from table(generator(rowcount => 3650)) -- Generates 10 years of dates
)

select
    to_number(to_char(date_actual, 'YYYYMMDD')) as date_key,
    date_actual,
    extract(year from date_actual) as year,
    extract(month from date_actual) as month,
    monthname(date_actual) as month_name,
    extract(day from date_actual) as day,
    dayname(date_actual) as day_of_week,
    case
        when dayname(date_actual) in ('Sat', 'Sun') then true
        else false
    end as is_weekend
from date_spine
