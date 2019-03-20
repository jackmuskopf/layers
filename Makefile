.PHONY: clean
clean:
	rm -rf ./python && mkdir ./python

.PHONY: install
install:
	pip install -r requirements.txt --target=python

.PHONY: package
package:
	cp -a package/* python/

.PHONY: zip
zip:
	zip -r9 python.zip ./python

.PHONY: push
push:
	zip -r9 python.zip ./python \
		&& aws lambda publish-layer-version \
			--layer-name ${name} \
			--zip-file fileb://python.zip \
			--compatible-runtimes python3.6 python3.7
