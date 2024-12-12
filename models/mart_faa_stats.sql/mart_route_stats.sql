WITH routes AS (
    SELECT origin
           , dest
           , count(*) AS n_flights_per_route
           , count(DISTINCT tail_number) AS arr_tail
           , count(DISTINCT airline) AS nunique_airlines
           , avg(actual_elapsed_time) AS avg_elapsed_time
           , avg(arr_delay) AS avg_delay
           , max(arr_delay) AS max_arr_delay
           , max(dep_delay) AS max_dep_delay
           , min(arr_delay) AS min_arr_delay
           , min(dep_delay) AS min_dep_delay
           , sum(cancelled) AS cancelled_tot
           , sum(diverted) AS diverted_tot
    FROM {ref{"prep_flights"}}
    GROUP BY origin, dest
), routes_join_origin AS (
    SELECT 
     	a.city AS origin_city
     	, a.city AS origin_country
     	, a.name AS origin_name
     	FROM routes r
    JOIN {ref{"prep_airport"}}
    ON r.origin = a.faa
)
SELECT *
FROM routes_join_origin;
JOIN {ref{"prep_airport"}}
on rjo.dest = a.faa


