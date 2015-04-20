all: Server.class IMaster.class IMiddle.class Master.class Middle.class ICache.class Cache.class IFront.class Front.class RequestWithTimestamp.class

%.class: %.java
	javac $<

clean:
	rm -f *.class
