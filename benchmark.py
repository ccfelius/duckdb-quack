import subprocess
import os
import time
import math
import statistics

duckdb_binary = './build/release/duckdb'
print('method\tthreads\trows\tmedian_time_seconds')

for duckdb_socket in ['quack:localhost']:
#, '/tmp/duckdb-rpc-socket']:
	server = subprocess.Popen([duckdb_binary, '-init', '/dev/null', '-cmd', f"CALL quack_serve('{duckdb_socket}', token='asdf')",  '-readonly', '~/nobackup/tpch-sf100.db'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
	# wait for socket to actually be open
	time.sleep(1)
	for threads in [8, 1,2,4]:
		for rows in [int(math.pow(10,y)) for y in range(2, 8)] + [59986052]:
			timings = []
			for rep in range(3):
				start = time.time()
				client = subprocess.run([duckdb_binary,  '-init', '/dev/null',  '-c', f"SET enable_progress_bar = false; set threads={threads}; EXPLAIN ANALYZE FROM quack_query('{duckdb_socket}','FROM lineitem LIMIT {rows}', token='asdf');"], stdout=subprocess.PIPE)
				timings.append(time.time() - start)
			median_time = round(statistics.median(timings), 3)
			print(f'{duckdb_socket}\t{threads}\t{rows}\t{median_time}')

	server.stdin.close()
	server.wait()