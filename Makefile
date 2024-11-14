.PHONY: install
install:
	./mvnw clean install -DskipTests -Drat.skip=true

.PHONY: install-benchmark
install-benchmark:
	./mvnw clean install -DskipTests -pl wayang-benchmark

.PHONY: package
package:
	./mvnw clean package -pl :wayang-assembly -Pdistribution