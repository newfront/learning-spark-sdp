-- A SQL materialized view that further filters the doubled even numbers.
-- Demonstrates referencing other datasets defined in the pipeline by name.
CREATE MATERIALIZED VIEW large_doubled_even_numbers AS
SELECT id, doubled
FROM doubled_even_numbers
WHERE doubled >= 20
