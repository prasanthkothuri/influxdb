package main

import (
        "fmt"
        "log"
        "math/rand"
        "os"
        "strconv"
        "time"
        "runtime"

        "github.com/influxdata/influxdb/client/v2"
)

func Query() {
        con, err := client.NewHTTPClient(client.HTTPConfig{Addr: "http://localhost:8086"})
        if err != nil {
                log.Fatal(err)
        }

        q := client.Query{
                Command:  "select count(value) from shapes",
                Database: "benchmark",
        }
        if response, err := con.Query(q); err == nil && response.Error() == nil {
                log.Println(response.Results)
        }
}

func Write(sSize int, bSize int) {

        //fmt.Printf("Writing to Influx")

        conf := client.HTTPConfig{
                Addr:      "http://localhost:8086",
                Username: os.Getenv("INFLUX_USER"),
                Password: os.Getenv("INFLUX_PWD"),
        }
        con, err := client.NewHTTPClient(conf)

        if err != nil {
                log.Fatal(err)
        }

        bps, err := client.NewBatchPoints(client.BatchPointsConfig{
                Database:        "benchmark",
                RetentionPolicy: "default",
                Precision: "ns",
        })


        var (
                shapes     = []string{"circle", "rectangle", "square", "triangle"}
                colors     = []string{"red", "blue", "green"}
                sampleSize = sSize
                batchSize  = bSize
                batches    = sampleSize / batchSize
        )

        log.Printf("Writing %v points", sampleSize)
        for j := 0; j < batches; j++ {

        rdom := int64(j)*7
        rand.Seed(rdom)

        for i := 0; i < batchSize; i++ {

                tags := map[string]string{
                                "color": strconv.Itoa(rand.Intn(len(colors))),
                                "shape": strconv.Itoa(rand.Intn(len(shapes))),
                        }
                fields :=  map[string]interface{}{
                            "value": rand.Intn(batchSize),
                           }
                pts,err := client.NewPoint(
                        "shapes",
                        tags,
                        fields,
                        time.Now(),
                )

               if err != nil {
                 log.Fatalln("Error: ", err)
               }
               bps.AddPoint(pts)
        }

        //log.Printf("Writing %v batch", j)
        con.Write(bps)

        if err != nil {
                fmt.Printf("Errors")
                log.Fatal(err)
        }

       //log.Printf("Batch %v completed!", j)

       }
       log.Printf("Wrote %v points", sampleSize)
}

func main() {

        //f, err := os.Create("/tmp/abc.out")
        //if err != nil {
        //    log.Fatal(err)
        //}
        //pprof.StartCPUProfile(f)
        //defer pprof.StopCPUProfile()


        c, err := strconv.Atoi(os.Args[1])
        s, err := strconv.Atoi(os.Args[2])
        b, err := strconv.Atoi(os.Args[3])

        runtime.GOMAXPROCS(c)

        for i := 0; i < c; i++ {
        go Write(s,b)
        }

        var input string
        fmt.Scanln(&input)
        fmt.Printf("%v",err)

        //Write()

}

