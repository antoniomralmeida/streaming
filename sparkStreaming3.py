import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == "__main__":

	sc = SparkContext(master="local[2]", appName="StreamingErrorCount")
	ssc = StreamingContext(sc, 2) # 2 segundos de intervalo de tempo

	ssc.checkpoint("file:///home/felipe/checkpoint2") # tolerancia a falhas
	
	# lines eh uma sequencia de RDDs
	lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) # host e porta

	counts = lines.countByWindow(10,2) # (window size, sliding interval) 

	counts.pprint() # imprime para cada intervalo. nao sao necessarios loops

	ssc.start() # inicia a escuta peloas dados de streaming
	ssc.awaitTermination() # aplicacao espera terminar os dados de transmissao 
