package main

import (
  "crypto/sha1"
  "encoding/hex"
  "encoding/json"
  "fmt"
  "log"
  "net/http"
  "os"
  "strconv"
  "time"
  //"github.com/darkhelmet/twitterstream"
  cfenv "github.com/cloudfoundry-community/go-cfenv"
  "github.com/codegangsta/negroni"
  "github.com/gorilla/mux"
  "github.com/unrolled/render"
  "github.com/mitchellh/goamz/aws"
  "tweetstream/twitterstream"
  "tweetstream/s3"
)

var s3Bucket *s3.Bucket
var client *twitterstream.Client
var wait = 1
var maxWait = 600 // Seconds
var rendering *render.Render
var quitChannels map[string]chan struct{}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func int64toString(value int64) (string) {
	return strconv.FormatInt(value, 10)
}

type appError struct {
	err error
	status int
	json string
	template string
	binding interface{}
}

type appHandler func(http.ResponseWriter, *http.Request) *appError

func (fn appHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
  if e := fn(w, r); e != nil {
		log.Print(e.err)
		if e.status != 0 {
			if e.json != "" {
				rendering.JSON(w, e.status, e.json)
			} else {
				rendering.HTML(w, e.status, e.template, e.binding)
			}
		}
  }
}

func RecoverHandler(next http.Handler) http.Handler {
  fn := func(w http.ResponseWriter, r *http.Request) {
    defer func() {
      if err := recover(); err != nil {
        log.Printf("panic: %+v", err)
        http.Error(w, http.StatusText(500), 500)
      }
    }()

    next.ServeHTTP(w, r)
  }
	return http.HandlerFunc(fn)
}

func Index(w http.ResponseWriter, r *http.Request) {
  rendering.HTML(w, http.StatusOK, "index", nil)
}

func main() {
  quitChannels = make(map[string]chan struct{})
  var port = ""
  var s3Auth aws.Auth
  var s3SpecialRegion  aws.Region
  var s3BucketName string
  _, err := cfenv.Current()
  if(err != nil) {
    port = "80"
    s3Auth = aws.Auth{
      AccessKey: os.Getenv("S3_ACCESS_KEY"),
      SecretKey: os.Getenv("S3_SECRET_KEY"),
    }
    s3SpecialRegion = aws.Region{
      Name: "Special",
      S3Endpoint: os.Getenv("S3_ENDPOINT"),
    }
    s3BucketName = os.Getenv("S3_BUCKET")
    client = twitterstream.NewClient(os.Getenv("TWITTER_CONSUMER_KEY"), os.Getenv("TWITTER_CONSUMER_SECRET"), os.Getenv("TWITTER_ACCESS_TOKEN"), os.Getenv("TWITTER_ACCESS_SECRET"))
  } else {
    cfenv := cfenv.CurrentEnv()
    port = os.Getenv("PORT")
    s3Auth = aws.Auth{
      AccessKey: cfenv["S3_ACCESS_KEY"],
      SecretKey: cfenv["S3_SECRET_KEY"],
    }
    s3SpecialRegion = aws.Region{
      Name: "Special",
      S3Endpoint: cfenv["S3_ENDPOINT"],
    }
    s3BucketName = cfenv["S3_BUCKET"]
    client = twitterstream.NewClient(cfenv["TWITTER_CONSUMER_KEY"], cfenv["TWITTER_CONSUMER_SECRET"], cfenv["TWITTER_ACCESS_TOKEN"], cfenv["TWITTER_ACCESS_SECRET"])
  }

  s3Client := s3.New(s3Auth, s3SpecialRegion)
  s3Bucket = s3Client.Bucket(s3BucketName)

  go gc()

  // See http://godoc.org/github.com/unrolled/render
  rendering = render.New(render.Options{Directory: "app/templates"})

  // See http://www.gorillatoolkit.org/pkg/mux
  router := mux.NewRouter()
  router.HandleFunc("/", Index).Methods("GET")
  router.Handle("/api/v1/start", appHandler(Start))
  router.Handle("/api/v1/stop", appHandler(Stop))
	router.PathPrefix("/app/").Handler(http.StripPrefix("/app/", http.FileServer(http.Dir("app"))))

	n := negroni.Classic()
	n.UseHandler(RecoverHandler(router))
	//http.ListenAndServeTLS(":" + port, "fe1b47ba5bcb246b.crt", "connectspeople.com.key", n)
	n.Run(":" + port)

	fmt.Printf("Listening on port " + port)
}

func Start(w http.ResponseWriter, r *http.Request) *appError {
	decoder := json.NewDecoder(r.Body)
	var s map[string]string
	err := decoder.Decode(&s)
	if(err != nil) {
		fmt.Println(err)
	}

  userS3Auth := aws.Auth{
    AccessKey: s["accesskey"],
    SecretKey: s["secretkey"],
  }

  userS3SpecialRegion := aws.Region{
    Name: "Special",
    S3Endpoint: s["endpoint"],
  }

	userS3BucketName := s["bucket"]

  userS3Client := s3.New(userS3Auth, userS3SpecialRegion)
  userS3Bucket := userS3Client.Bucket(userS3BucketName)

  userS3ObjectSize, err := strconv.Atoi(s["objectsize"])
  if err != nil {
    log.Fatal(err)
  }

  userKeywords := s["keywords"]

  if(isJobRunning(userS3Bucket, userKeywords)) {
    return &appError{err: err, status: http.StatusForbidden, json: "Job already running"}
  } else {
    writeJobObject(userS3Bucket, userKeywords)
    jobChannel := make(chan []byte)
    str := s["accesskey"] + s["secretkey"] + s["bucket"] + s["keywords"]
    h := sha1.New()
    h.Write([]byte(str))
    hString := hex.EncodeToString(h.Sum(nil))
    quitChannel := make(chan struct{})
    quitChannels[hString] = quitChannel
    go worker(quitChannel, jobChannel, userS3ObjectSize, userS3Bucket, userKeywords)
    go job(quitChannel, jobChannel, client, userKeywords)
  }

  return nil
}

func Stop(w http.ResponseWriter, r *http.Request) *appError {
	decoder := json.NewDecoder(r.Body)
	var s map[string]string
	err := decoder.Decode(&s)
	if(err != nil) {
		fmt.Println(err)
	}
  str := s["accesskey"] + s["secretkey"] + s["bucket"] + s["keywords"]
  h := sha1.New()
  h.Write([]byte(str))
  hString := hex.EncodeToString(h.Sum(nil))
  _, err = s3Bucket.Get("job_" + hString)
  if(err != nil) {
    return &appError{err: err, status: http.StatusForbidden, json: "Job already stopped, but you need to wait up to one minute before being able to start it again"}
  } else {
    err = s3Bucket.Put("job_" + hString + "_stop", nil, "text/plain", "")
    if err != nil {
      return &appError{err: err, status: http.StatusInternalServerError, json: "Can't stop the job"}
    }
  }

  return nil
}

func gc() {
  for {
    log.Printf("GC jobs stopped or not updated since more than one hour")
    r, err := s3Bucket.List("job_", "", "", 0)
    if(err != nil) {
      fmt.Println("Can't execute GC")
    } else {
      if(len(r.Contents) > 0) {
        for _, item := range r.Contents {
          if item.Key[len(item.Key) - 5:] == "_stop" {
            hString := item.Key[4:len(item.Key) - 5]
            if _, ok := quitChannels[hString]; ok {
              close(quitChannels[hString])
              delete(quitChannels, hString)
              time.Sleep(5 * time.Second)
              err = s3Bucket.Del(item.Key)
              if err != nil {
                fmt.Println("GC: Can't delete job stopped object " + item.Key)
              }
              err := s3Bucket.Del("job_" + hString)
              if err != nil {
                fmt.Println("GC: Can't delete job object " + item.Key)
              }
            }
          } else {
            data, err := s3Bucket.Get(item.Key)
            if(err != nil) {
              fmt.Println("GC: Can't retrieve information about the job")
            } else {
              timestamp, err := strconv.ParseInt(string(data), 10, 64)
              if err != nil {
                fmt.Println("GC: Job information in bad format")
              }
              now := time.Now().UnixNano()
              diff := (now - timestamp) / (1000 * 1000 * 1000)
              if diff > 3600 {
                fmt.Println("GC: Delete job object " + item.Key)
                err := s3Bucket.Del(item.Key)
                if err != nil {
                  fmt.Println("GC: Can't delete job object " + item.Key)
                }
              }
            }
          }
        }
      } else {
        fmt.Println("GC: No jobs")
      }
    }
    time.Sleep(10 * time.Second)
  }
}

func job(quitChannel chan struct{}, jobChannel chan []byte, client *twitterstream.Client, userKeywords string) {
  for {
    select {
      default:
      log.Printf("tracking keywords %s", userKeywords)
      conn, err := client.Track(userKeywords)
      if err != nil {
        log.Printf("tracking failed: %s", err)
        wait = wait << 1
        log.Printf("waiting for %d seconds before reconnect", min(wait, maxWait))
        time.Sleep(time.Duration(min(wait, maxWait)) * time.Second)
        continue
      } else {
        wait = 1
      }
      decodeTweets(quitChannel, jobChannel, conn)
      case _ = <-quitChannel:
      return
    }
  }
}

func isJobRunning(userS3Bucket *s3.Bucket, userKeywords string) bool {
  str := userS3Bucket.S3.Auth.AccessKey + userS3Bucket.S3.Auth.SecretKey + userS3Bucket.Name + userKeywords
  h := sha1.New()
  h.Write([]byte(str))
  hString := hex.EncodeToString(h.Sum(nil))
  _, err := s3Bucket.Get("job_" + hString)
  if err != nil {
    return false
  } else {
    return true
  }
}

func writeJobObject(userS3Bucket *s3.Bucket, userKeywords string) {
  timestamp := int64toString(time.Now().UnixNano())
  s := userS3Bucket.S3.Auth.AccessKey + userS3Bucket.S3.Auth.SecretKey + userS3Bucket.Name + userKeywords
  h := sha1.New()
  h.Write([]byte(s))
  hString := hex.EncodeToString(h.Sum(nil))
  err := s3Bucket.Put("job_" + hString, []byte(timestamp), "text/plain", "")
  if err != nil {
    fmt.Println("ERROR: Can't write/update the job object")
  }
}

func writeObject(data []byte, userS3Bucket *s3.Bucket, userKeywords string) {
  timestamp := int64toString(time.Now().UnixNano())
  err := userS3Bucket.Put("keywords_" + userKeywords + "_" + timestamp + ".txt", data, "text/plain", "")
  if err != nil {
    fmt.Println("ERROR: Can't write the object")
  }
  writeJobObject(userS3Bucket, userKeywords)
}

func worker(quitChannel chan struct{}, jobChannel chan []byte, size int, userS3Bucket *s3.Bucket, userKeywords string) {
  buffer := make([]byte, size)
  position := 0
  for {
    select {
      default:
      entry := <- jobChannel
      length := len(entry)
      if length > size  {
        writeObject(entry, userS3Bucket, userKeywords)
      }
      if (length + position) > size {
        writeObject(buffer, userS3Bucket, userKeywords)
        position = 0
      }
      copy(buffer[position:], entry)
      position += length
      case _ = <-quitChannel:
      return
    }
  }
}

func decodeTweets(quitChannel chan struct{}, jobChannel chan []byte, conn *twitterstream.Connection) {
  for {
    select {
      default:
      if tweet, err := conn.Next(); err == nil {
        //fmt.Println(tweet.Text)

        jobChannel <- []byte(tweet.Text)
      } else {
        log.Printf("decoding tweet failed: %s", err)
        conn.Close()
        return
      }
      case _ = <-quitChannel:
      return
    }

  }
}
