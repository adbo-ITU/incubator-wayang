.PHONY: install-api
install-api:
	./mvnw clean install -DskipTests -pl wayang-api

.PHONY: install-platforms
install-platforms:
	./mvnw clean install -DskipTests -pl wayang-platforms

.PHONY: package
package:
	./mvnw clean package -pl :wayang-assembly -Pdistribution