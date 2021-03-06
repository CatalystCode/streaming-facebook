A library for reading social data from [Facebook](https://www.facebook.com) using Spark Streaming.

[![Travis CI status](https://api.travis-ci.org/CatalystCode/streaming-facebook.svg?branch=master)](https://travis-ci.org/CatalystCode/streaming-facebook)

## Usage example ##

Run a demo via:

```sh
# set up all the requisite environment variables
# you can create a new app id and secret here: https://developers.facebook.com/quickstarts/
# you can generate a new auth token here: https://developers.facebook.com/tools/accesstoken/
export FACEBOOK_APP_ID="..."
export FACEBOOK_APP_SECRET="..."
export FACEBOOK_AUTH_TOKEN="..."

# compile scala, run tests, build fat jar
sbt assembly

# run locally
java -cp target/scala-2.11/streaming-facebook-assembly-0.0.3.jar FacebookDemo standalone

# run on spark
spark-submit --class FacebookDemo --master local[2] target/scala-2.11/streaming-facebook-assembly-0.0.3.jar spark
```

## How does it work? ##

Facebook doesn't expose a firehose API so we resort to polling. The FacebookReceiver pings the Facebook API every few
seconds and pushes any new posts into Spark Streaming for further processing.

Currently, the following ways to read Facebook items are supported:
- by page ([sample data](https://www.facebook.com/pg/aljazeera/posts/))
- comments for page ([sample data](https://www.facebook.com/forbes/posts/10155598401707509?comment_id=10155598513972509))

## Release process ##

1. Configure your credentials via the `SONATYPE_USER` and `SONATYPE_PASSWORD` environment variables.
2. Update `version.sbt`
3. Enter the SBT shell: `sbt`
4. Run `sonatypeOpen "enter staging description here"`
5. Run `publishSigned`
6. Run `sonatypeRelease`
