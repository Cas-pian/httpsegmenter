all:
	gcc -Wall -std=c99 -g segmenter.c -o segmenter -L/usr/local/lib -lavformat -lavcodec -lavutil -lm -lz -ljson-c

clean:
	rm segmenter

install: 
	chmod +x segmenter
	cp -f segmenter /usr/local/bin/
	chmod +x segmenterdemon.sh
	cp -f segmenterdemon.sh /usr/local/bin/

uninstall:
	rm /usr/local/bin/segmenter
