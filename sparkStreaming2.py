import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


if __name__ == "__main__":

	sc = SparkContext(master="local[2]", appName="StreamingErrorCount")
	ssc = StreamingContext(sc, 2) # 2 segundos de intervalo de tempo

	ssc.checkpoint("file:///home/felipe/checkpoint2") # tolerancia a falhas
	
	# lines eh uma sequencia de RDDs
	lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2])) # host e porta

	# conta palavras
	def countWords(newValues, lastSum):
		if lastSum is None: # inicializa com zero se nao houver ultima soma
			lastSum = 0
		return sum(newValues, lastSum)

	# contagem de erros
	word_counts = lines.flatMap(lambda line: line.split(" "))\
			.map(lambda word: (word, 1))\
			.updateStateByKey(countWords) # key = word. soma todos os valores associados a mesma chave.

	word_counts.pprint() # imprime para cada intervalo. nao sao necessarios loops

	ssc.start() # inicia a escuta peloas dados de streaming
	ssc.awaitTermination() # aplicacao espera terminar os dados de transmissao 
