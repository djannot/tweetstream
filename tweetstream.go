package main

import (
  "crypto/sha1"
  "encoding/hex"
  "encoding/json"
  "fmt"
  "log"
  "net/http"
  "os"
  "regexp"
  "strconv"
  "strings"
  "time"
  cfenv "github.com/cloudfoundry-community/go-cfenv"
  "github.com/codegangsta/negroni"
  "github.com/gorilla/mux"
  "github.com/unrolled/render"
  "github.com/mitchellh/goamz/aws"
  "github.com/djannot/tweetstream/twitterstream"
  "github.com/djannot/tweetstream/s3"
  //"github.com/mitchellh/goamz/s3"
  //"github.com/darkhelmet/twitterstream"
)

var s3Bucket *s3.Bucket
var wait = 1
var maxWait = 600 // Seconds
var jobPrefix = "job"
var jobStopSuffix = "stop"
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
  hostname, _ := os.Hostname()
  rendering.HTML(w, http.StatusOK, "index", hostname)
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
  } else {
    cfenv := cfenv.CurrentEnv()
    port = os.Getenv("PORT")
    //port = os.Getenv("VCAP_APP_PORT")
    s3Auth = aws.Auth{
      AccessKey: cfenv["S3_ACCESS_KEY"],
      SecretKey: cfenv["S3_SECRET_KEY"],
    }
    s3SpecialRegion = aws.Region{
      Name: "Special",
      S3Endpoint: cfenv["S3_ENDPOINT"],
    }
    s3BucketName = cfenv["S3_BUCKET"]
  }

  s3Client := s3.New(s3Auth, s3SpecialRegion)
  s3Bucket = s3Client.Bucket(s3BucketName)

  go gc()

  // See http://godoc.org/github.com/unrolled/render
  rendering = render.New(render.Options{Directory: "app/templates"})

  // See http://www.gorillatoolkit.org/pkg/mux
  router := mux.NewRouter()
  router.HandleFunc("/", Index).Methods("GET")
  router.Handle("/api/v1/start", appHandler(Start)).Methods("POST")
  router.Handle("/api/v1/stop", appHandler(Stop)).Methods("POST")
  router.Handle("/api/v1/status", appHandler(Status)).Methods("POST")
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

  client := twitterstream.NewClient(s["twitterconsumerkey"],s["twitterconsumersecret"],s["twitteraccesstoken"],s["twitteraccesssecret"])

  if(isJobRunning(userS3Bucket, userKeywords)) {
    return &appError{err: err, status: http.StatusForbidden, json: "Job already running"}
  } else {
    writeJobObject(userS3Bucket, userKeywords)
    jobChannel := make(chan []byte)
    str := s["accesskey"] + s["secretkey"]
    h := sha1.New()
    h.Write([]byte(str))
    hString := hex.EncodeToString(h.Sum(nil))
    key := hString + "_" + s["bucket"] + "." + s["keywords"]
    quitChannel := make(chan struct{})
    quitChannels[key] = quitChannel
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
  str := s["accesskey"] + s["secretkey"]
  h := sha1.New()
  h.Write([]byte(str))
  hString := hex.EncodeToString(h.Sum(nil))
  key := hString + "_" + s["bucket"] + "." + s["keywords"]
  _, err = s3Bucket.Get(jobPrefix + "_" + key)
  if(err != nil) {
    return &appError{err: err, status: http.StatusForbidden, json: "Job already stopped, but you need to wait up to one hour before being able to start it again if an instance of the application has crashed"}
  } else {
    err = s3Bucket.Put(jobPrefix + "_" + key + "_" + jobStopSuffix, nil, "text/plain", s3.ACL("bucket-owner-full-control"))
    if err != nil {
      return &appError{err: err, status: http.StatusInternalServerError, json: "Can't stop the job"}
    }
  }

  return nil
}

type StatusResponse struct {
  Bucket string `json:"bucket"`
  Keywords string `json:"keywords"`
	LastUpdated int64 `json:"lastupdated"`
}

func Status(w http.ResponseWriter, r *http.Request) *appError {
  var statusResponses []StatusResponse
	decoder := json.NewDecoder(r.Body)
	var s map[string]string
	err := decoder.Decode(&s)
	if(err != nil) {
		fmt.Println(err)
	}
  str := s["accesskey"] + s["secretkey"]
  h := sha1.New()
  h.Write([]byte(str))
  hString := hex.EncodeToString(h.Sum(nil))
  resp, err := s3Bucket.List(jobPrefix + "_" + hString + "_", "", "", 0)
  if(err != nil) {
    return &appError{err: err, status: http.StatusInternalServerError, json: "Can't retrieve the job list"}
  } else {
    if(len(resp.Contents) > 0) {
      for _, item := range resp.Contents {
        if item.Key[len(item.Key) - (len(jobStopSuffix) + 1):] != "_" + jobStopSuffix {
          regex, _ := regexp.Compile(`job_.*_(.*)`)
          bucketKeywords := regex.FindStringSubmatch(item.Key)[1]
          bucket := bucketKeywords[:strings.Index(bucketKeywords,".")]
	        keywords := bucketKeywords[strings.Index(bucketKeywords,".") + 1:]
          var diff int64
          data, err := s3Bucket.Get(item.Key)
          if(err != nil) {
            fmt.Println("Status: Can't retrieve information about the job")
          } else {
            timestamp, err := strconv.ParseInt(string(data), 10, 64)
            if err != nil {
              fmt.Println("Status: Job information in bad format")
            }
            now := time.Now().UnixNano()
            diff = (now - timestamp) / (1000 * 1000 * 1000)
          }
          var statusResponse = StatusResponse{
            Bucket: bucket,
            Keywords: keywords,
            LastUpdated: diff,
          }
          statusResponses = append(statusResponses, statusResponse)
        }
      }
    }
    rendering.JSON(w, http.StatusOK, statusResponses)
  }

  return nil
}

func gc() {
  for {
    log.Printf("GC jobs stopped or not updated since more than one hour")
    r, err := s3Bucket.List(jobPrefix + "_", "", "", 0)
    if(err != nil) {
      fmt.Println("Can't execute GC")
    } else {
      if(len(r.Contents) > 0) {
        for _, item := range r.Contents {
          if item.Key[len(item.Key) - (len(jobStopSuffix) + 1):] == "_" + jobStopSuffix {
            key := item.Key[(len(jobPrefix) + 1):len(item.Key) - (len(jobStopSuffix) + 1)]
            if _, ok := quitChannels[key]; ok {
              close(quitChannels[key])
              delete(quitChannels, key)
              time.Sleep(5 * time.Second)
              err = s3Bucket.Del(item.Key)
              if err != nil {
                fmt.Println("GC: Can't delete job stopped object " + item.Key)
              }
              err := s3Bucket.Del(jobPrefix + "_" + key)
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
  str := userS3Bucket.S3.Auth.AccessKey + userS3Bucket.S3.Auth.SecretKey
  h := sha1.New()
  h.Write([]byte(str))
  hString := hex.EncodeToString(h.Sum(nil))
  key := hString + "_" + userS3Bucket.Name + "." + userKeywords
  _, err := s3Bucket.Get(jobPrefix + "_" + key)
  if err != nil {
    return false
  } else {
    return true
  }
}

func writeJobObject(userS3Bucket *s3.Bucket, userKeywords string) {
  timestamp := int64toString(time.Now().UnixNano())
  s := userS3Bucket.S3.Auth.AccessKey + userS3Bucket.S3.Auth.SecretKey
  h := sha1.New()
  h.Write([]byte(s))
  hString := hex.EncodeToString(h.Sum(nil))
  key := hString + "_" + userS3Bucket.Name + "." + userKeywords
  err := s3Bucket.Put(jobPrefix + "_" + key, []byte(timestamp), "text/plain", s3.ACL("bucket-owner-full-control"))
  if err != nil {
    fmt.Println("ERROR: Can't write/update the job object")
  }
}

func writeObject(data []byte, userS3Bucket *s3.Bucket, userKeywords string) {
  timestamp := int64toString(time.Now().UnixNano())
  err := userS3Bucket.Put("/data/keywords_" + userKeywords + "_" + timestamp + ".txt", data, "text/plain", s3.ACL("public-read-write"))
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
      entry = append(entry, []byte("\n")...)
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
      if tweet, err := conn.NextRaw(); err == nil {
        //fmt.Println(tweet.Text)

        jobChannel <- tweet
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
