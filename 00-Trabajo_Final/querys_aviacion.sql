-- Ejercicio 06
SELECT COUNT (*) from aeropuerto_tabla a
WHERE a.fecha >= '2021-12-01' and a.fecha <= '2022-01-31'
	and a.tipo_de_movimiento == 'Despegue'
;
-- Ejercicio 07
SELECT SUM(a.pasajeros) from aeropuerto_tabla a
WHERE a.fecha >= '2021-01-01' and a.fecha <= '2022-06-30'
	and a.tipo_de_movimiento == 'Despegue'
    and a.aerolinea_nombre == 'AEROLINEAS ARGENTINAS SA'
;
-- Ejercicio 08
SELECT
	a.fecha, a.horaUTC, a.aeropuerto as origen,
	adt.`ref` as ciudad_origen, a.origen_destino as destino,
	adt2.`ref` as ciudad_destino, a.pasajeros 
FROM aeropuerto_tabla as a 
	left join aeropuerto_detalles_tabla as adt
	on a.aeropuerto == adt.aeropuerto 
	left join aeropuerto_detalles_tabla as adt2
	on a.origen_destino == adt2.aeropuerto 
WHERE a.fecha >= '2022-01-01' and a.fecha <= '2022-06-30'
	and a.tipo_de_movimiento == 'Despegue'
ORDER by a.fecha desc, a.horautc desc
;
-- Ejercicio 09
SELECT
	SUM(a.pasajeros) as total_pasajeros,
	a.aerolinea_nombre
FROM aeropuerto_tabla as a 
WHERE a.fecha >= '2021-01-01' and a.fecha <= '2022-06-30'
	and a.tipo_de_movimiento == 'Despegue'
	and a.aerolinea_nombre != '0'
GROUP by a.aerolinea_nombre
SORT BY total_pasajeros desc
LIMIT 10
;
-- Ejercicio 10
SELECT
	COUNT(*) as total_vuelos,
	at2.aeronave
FROM (
	SELECT * from aeropuerto_tabla as a
	WHERE a.fecha >= '2021-01-01' and a.fecha <= '2022-06-30'
    	and a.tipo_de_movimiento == 'Despegue'
	    and (a.aeropuerto == 'EZE' or a.aeropuerto == 'AER')
	    and a.aeronave != '0'
    ) as at2
GROUP by at2.aeronave 
SORT BY total_vuelos desc
LIMIT 10
;
