-- A SQL materialized view that further filters the doubled even numbers.
-- Demonstrates referencing other datasets defined in the pipeline by name.
create materialized view large_doubled_even_numbers as
select id, doubled
from doubled_even_numbers
where doubled >= 20
