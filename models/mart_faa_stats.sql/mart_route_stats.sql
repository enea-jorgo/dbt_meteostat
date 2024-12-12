WITH routes AS (
    SELECT 
        origin,
        dest,
        count(*) AS n_flights_per_route,
        count(DISTINCT tail_number) AS arr_tail,
        count(DISTINCT airline) AS nunique_airlines,
        avg(actual_elapsed_time) AS avg_elapsed_time,
        avg(arr_delay) AS avg_delay,
        max(arr_delay) AS max_arr_delay,
        max(dep_delay) AS max_dep_delay,
        min(arr_delay) AS min_arr_delay,
        min(dep_delay) AS min_dep_delay,
        sum(cancelled) AS cancelled_tot,
        sum(diverted) AS diverted_tot
    FROM {{ ref("prep_flights") }}
    GROUP BY origin, dest
), 
routes_join_origin AS (
    SELECT 
        r.origin,
        r.dest,
        r.n_flights_per_route,
        r.arr_tail,
        r.nunique_airlines,
        r.avg_elapsed_time,
        r.avg_delay,
        r.max_arr_delay,
        r.max_dep_delay,
        r.min_arr_delay,
        r.min_dep_delay,
        r.cancelled_tot,
        r.diverted_tot,
        a.city AS origin_city,
        a.country AS origin_country,
        a.name AS origin_name
    FROM routes r
    JOIN {{ ref("prep_airport") }} a
    ON r.origin = a.faa
),
routes_with_dest AS (
    SELECT 
        rjo.*,
        a.city AS dest_city,
        a.country AS dest_country,
        a.name AS dest_name
    FROM routes_join_origin rjo
    JOIN {{ ref("prep_airport") }} a
    ON rjo.dest = a.faa
)
SELECT *
FROM routes_with_dest;
