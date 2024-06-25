-- Ejercicio 5.a
SELECT COUNT(*) 
FROM car_rental_analytics cra
WHERE fueltype != 'gasoline'
	and rating >= 4
;
-- Ejercicio 5.b
SELECT COUNT (*) as total_alquileres, cra.name_state 
FROM car_rental_analytics cra
GROUP BY cra.name_state
ORDER BY total_alquileres asc
LIMIT 5
;
-- Ejercicio 5.c

-- Ejercicio 5.d

-- Ejercicio 5.e

-- Ejercicio 5.f
