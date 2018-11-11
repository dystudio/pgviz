SELECT customer.c_name, COUNT(DISTINCT orders.o_orderkey)
FROM customer, orders
WHERE customer.c_custkey = orders.o_custkey AND orders.o_totalprice >= 30000
GROUP BY customer.c_name
HAVING COUNT(DISTINCT orders.o_orderkey) >= 20
ORDER BY COUNT(DISTINCT orders.o_orderkey) DESC
LIMIT 10;