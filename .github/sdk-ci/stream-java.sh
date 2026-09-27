set -eu
test ! -e /work
mkdir /results/classes
classpath=$(cat /results/classpath)
javac -cp "$classpath" -d /results/classes /sdk/conformance/stream-java/Consumer.java
REACON_RETAINED_JAR="/artifacts/reacon-java-$REACON_SDK_PACKAGE_VERSION.jar" REACON_EXPECTED_JAVA_MAJOR=21 \
  java -cp "/results/classes:$classpath" Consumer
