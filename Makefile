build:
	docker build \
		-t fiap_spark_streamming .
.PHONY: build

run: 
	docker run \
		-it \
		-v ${PWD}:/local_workspace \
		--rm \
		--name fiap-spark-streamming fiap_spark_streamming
.PHONY: run
