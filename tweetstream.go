package main

import (
  "fmt"
  "log"
  "os"
  "strconv"
  "time"
  //"github.com/darkhelmet/twitterstream"
  cfenv "github.com/cloudfoundry-community/go-cfenv"
  "github.com/mitchellh/goamz/aws"
  "tweetstream/twitterstream"
  "tweetstream/s3"
)

var s3Bucket *s3.Bucket
var wait = 1
var maxWait = 600 // Seconds

var channel = make(chan []byte)
/*
func init() {
  go worker(8092)
}
*/

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func int64toString(value int64) (string) {
	return strconv.FormatInt(value, 10)
}

func writeObject(data []byte) {
  timestamp := time.Now()
	objectUuid := int64toString(timestamp.UnixNano())
  err := s3Bucket.Put(objectUuid + ".txt", data, "text/plain", "")
  if err != nil {
    fmt.Println("ERROR: Can't write the object")
  }
}

func worker(size int) {
  buffer := make([]byte, size)
  position := 0
  for {
    entry := <- channel
    length := len(entry)
    if length > size  {
      writeObject(entry)
    }
    if (length + position) > size {
      writeObject(buffer)
      position = 0
    }
    copy(buffer[position:], entry)
    position += length
  }
}

func decodeTweets(conn *twitterstream.Connection) {
  for {
    if tweet, err := conn.Next(); err == nil {
      //fmt.Println(tweet.Text)
      channel <- []byte(tweet.Text)
    } else {
      log.Printf("decoding tweet failed: %s", err)
      conn.Close()
      return
    }
  }
}

func main() {
  _, err := cfenv.Current()
  cfenv := cfenv.CurrentEnv()
  var s3Auth aws.Auth
  var s3SpecialRegion  aws.Region
  var s3BucketName string
  var client *twitterstream.Client
  var keywords string
  var s3ObjectSize int
  if(err != nil) {
    s3Auth = aws.Auth{
      AccessKey: os.Getenv("S3_ACCESS_KEY"),
      SecretKey: os.Getenv("S3_SECRET_KEY"),
    }
    s3SpecialRegion = aws.Region{
      Name: "Special",
      S3Endpoint: os.Getenv("S3_ENDPOINT"),
    }
    s3BucketName = os.Getenv("S3_BUCKET")
    s3ObjectSize, err = strconv.Atoi(os.Getenv("S3_OBJECT_SIZE"))
    if err != nil {
			log.Fatal(err)
		}
    client = twitterstream.NewClient(os.Getenv("TWITTER_CONSUMER_KEY"), os.Getenv("TWITTER_CONSUMER_SECRET"), os.Getenv("TWITTER_ACCESS_TOKEN"), os.Getenv("TWITTER_ACCESS_SECRET"))
    keywords = os.Getenv("TWITTER_KEYWORDS")
  } else {
    s3Auth = aws.Auth{
      AccessKey: cfenv["S3_ACCESS_KEY"],
      SecretKey: cfenv["S3_SECRET_KEY"],
    }
    s3SpecialRegion = aws.Region{
      Name: "Special",
      S3Endpoint: cfenv["S3_ENDPOINT"],
    }
    s3BucketName = cfenv["S3_BUCKET"]
    s3ObjectSize, err = strconv.Atoi(cfenv["S3_OBJECT_SIZE"])
    if err != nil {
			log.Fatal(err)
		}
    client = twitterstream.NewClient(cfenv["TWITTER_CONSUMER_KEY"], cfenv["TWITTER_CONSUMER_SECRET"], cfenv["TWITTER_ACCESS_TOKEN"], cfenv["TWITTER_ACCESS_SECRET"])
    keywords = cfenv["TWITTER_KEYWORDS"]
  }

  s3Client := s3.New(s3Auth, s3SpecialRegion)
  s3Bucket = s3Client.Bucket(s3BucketName)

  go worker(s3ObjectSize)

  for {
    log.Printf("tracking keywords %s", keywords)
    conn, err := client.Track(keywords)
    if err != nil {
      log.Printf("tracking failed: %s", err)
      wait = wait << 1
      log.Printf("waiting for %d seconds before reconnect", min(wait, maxWait))
      time.Sleep(time.Duration(min(wait, maxWait)) * time.Second)
      continue
    } else {
      wait = 1
    }
    decodeTweets(conn)
  }
}
