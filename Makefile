BUCKET := adf-bucket-3d05fc66-0f12-11ec-8eee-9cb6d0dc2783
ZIP_PREFIX := public/zips/
EMR_ZIP_NAME := emr_package.zip
LAMBDA_ZIP_NAME := lambda_package.zip

.DEFAULT_GOAL := zip-all

install:
	python setup.py install

zip-emr: local
	python bin/zip-emr.py -r . -o $(EMR_ZIP_NAME)
	aws s3 cp $(EMR_ZIP_NAME) s3://$(BUCKET)/$(ZIP_PREFIX)$(EMR_ZIP_NAME)
	aws s3api put-object-acl --bucket $(BUCKET) --key $(ZIP_PREFIX)$(EMR_ZIP_NAME) --acl public-read

zip-lambda: local
	python bin/zip-lambda.py -r . -o $(LAMBDA_ZIP_NAME)
	aws s3 cp $(LAMBDA_ZIP_NAME) s3://$(BUCKET)/$(ZIP_PREFIX)$(LAMBDA_ZIP_NAME)
	aws s3api put-object-acl --bucket $(BUCKET) --key $(ZIP_PREFIX)$(LAMBDA_ZIP_NAME) --acl public-read

zips: zip-emr zip-lambda

copy-data:
	cp config/flows/*.yaml src/ADF/data/config/flows/
	sed -i 's/ADF.funcs/flow_operations.operations/g' src/ADF/data/config/flows/*.yaml
	sed -i 's/BATCH_ID_COLUMN_NAME: MOD_ADF_BATCH_ID//g' src/ADF/data/config/flows/*.yaml
	sed -i 's/SQL_PK_COLUMN_NAME: MOD_ADF_ID//g' src/ADF/data/config/flows/*.yaml
	sed -i '/^$$/d' src/ADF/data/config/flows/*.yaml
	cp config/implementers/*.yaml src/ADF/data/config/implementers/
	sed -i '1s/^/extra_packages: [.]\n/' src/ADF/data/config/implementers/*.yaml
	sed -i 's/bucket: adf-bucket-3d05fc66-0f12-11ec-8eee-9cb6d0dc2783/bucket: YOUR-BUCKET-NAME-HERE/g' src/ADF/data/config/implementers/implementer.aws.yaml
	cp scripts/integ-tests/*.sh src/ADF/data/scripts/
	sed -i 's/EXE=bin\//EXE=/g' src/ADF/data/scripts/*.sh
	cp data_samples/*.csv src/ADF/data/data_samples/
	cp src/ADF/funcs.py src/ADF/data/pyfiles/operations.py

local: copy-data install

package: clean-local copy-data
	python setup.py sdist bdist_wheel

publish: package
	twine upload dist/*

zip-all: copy-data zips

clean-local:
	rm -rf build dist src/adf.egg-info
	find . | grep -E "(/__pycache__$$|\.pyc$$|\.pyo$$)" | xargs rm -rf
	rm -f $(EMR_ZIP_NAME)
	rm -f $(LAMBDA_ZIP_NAME)

clean-s3:
	aws s3 rm s3://$(BUCKET)/$(ZIP_PREFIX)$(EMR_ZIP_NAME)
	aws s3 rm s3://$(BUCKET)/$(ZIP_PREFIX)$(LAMBDA_ZIP_NAME)

clean: clean-local clean-s3

readme-toc:
	cat README.md | grep -E "^#{1,10} " | sed -E 's/(#+) (.+)/\1:\2:\2/g' | awk -F ":" '{ gsub(/#/,"  ",$$1); gsub(/[ ]/,"-",$$3); print $$1 "- [" $$2 "](#" tolower($$3) ")" }'
