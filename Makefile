sanandres=627
sanpedro=771
puebla=580
MUN=${sanpedro}

2: 1
	touch 2
	bash script.sh

1:
	touch 1
	curl -o file.zip "http://internet.contenidos.inegi.org.mx/contenidos/Productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/urbana/SHP_2/Puebla/702825315${MUN}_s.zip"
	unzip file.zip; rm file.zip; mv *.pdf ficha.pdf
	unzip "*.zip"
	rm *.zip

clean:
	rm 1 2 *.pdf; rm -r 21*
