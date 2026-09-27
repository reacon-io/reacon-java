set -eu
export REACON_EXPECTED_JAVA_MAJOR=21
sh /suite/java-package.sh
mkdir /results/stream-classes
classpath=$(cat /results/classpath)
javac -cp "$classpath" -d /results/stream-classes /sdk/conformance/stream-java/Consumer.java
REACON_TEST_URL="$REACON_STREAM_TEST_URL" REACON_RETAINED_JAR="/results/artifacts/reacon-java-$REACON_SDK_PACKAGE_VERSION.jar" java -cp "/results/stream-classes:$classpath" Consumer
