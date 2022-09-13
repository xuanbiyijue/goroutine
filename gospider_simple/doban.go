package main

import (
	"fmt"
	"github.com/PuerkitoBio/goquery"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var db *sqlx.DB

const (
	USERNAME = "root"
	PASSWORD = "123456"
	HOST = "127.0.0.1"
	PORT = "3306"
	DBNAME = "db_goroutine"
)

// MovieData 要爬取的内容
type MovieData struct {
	Title    string `json:"title"`
	Director string `json:"director"`
	Picture  string `json:"picture"`
	Actor    string `json:"actor"`
	Year     string `json:"year"`
	Score    string `json:"score"`
	Quote    string `json:"quote"`
}

func main() {
	err := InitDB()
	if err != nil {
		log.Fatal("Init DB err: ", err)
	}
	ch := make(chan bool)
	start := time.Now()
	for i := 0; i < 10; i++ {
		go Spider(strconv.Itoa(i*25), ch)
	}
	for i := 0; i < 10; i++ {
		<-ch
	}
	cost := time.Since(start)
	fmt.Printf("cost=[%s]",cost)  // 2.2087926s -> 423.0853ms
	defer CloseDB()
}


func Spider(page string, ch chan bool) { //page string,ch chan bool
	// 构造客户端
	client := &http.Client{}

	// 构造http请求
	req, err := http.NewRequest("GET", "https://movie.douban.com/top250?start="+page, nil)
	if err != nil {
		log.Fatal(err)
	}
	// 设置请求头，防止浏览器检测爬虫访问。只需要把有的没的都放上来
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Cache-Control", "max-age=0")
	req.Header.Set("sec-ch-ua-mobile", "?0")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Referer", "https://movie.douban.com/chart")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9")

	// 发送http请求，获得response
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	// 解析网页
	docDetail, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	// 获取节点信息
	// #content > div > div.article > ol > li:nth-child(1)
	// #content > div > div.article > ol > li:nth-child(5)
	docDetail.Find("#content > div > div.article > ol > li > div").  // 返回一个列表
		Each(func(i int, s *goquery.Selection) {                     // 在列表中继续找
			var movieData MovieData
			//#content > div > div.article > ol > li:nth-child(16) > div > div.info > div.hd > a > span:nth-child(1)
			title := s.Find("div.info > div.hd > a > span:nth-child(1)").Text()
			img := s.Find("div.pic > a > img")  //获取到的是img的标签，需要的是标签里的src
			imgTmp, ok := img.Attr("src")
			// 相关信息
			info := strings.Trim(s.Find("div.info > div.bd > p:nth-child(1)").Text(), " ")
			// 从相关信息提取具体信息
			director, actor, year := InfoSpite(info)
			score := strings.Trim(s.Find("div.info > div.bd > div > span.rating_num").Text(), " ")
			score = strings.Trim(score, "\n")
			quote := strings.Trim(s.Find("div.info > div.bd > p.quote > span").Text(), " ")
			if ok {
				movieData.Title = title
				movieData.Director = director
				movieData.Picture = imgTmp
				movieData.Actor = actor
				movieData.Year = year
				movieData.Score = score
				movieData.Quote = quote
				fmt.Println(movieData)
				if InsertData(movieData) {
					fmt.Println("Saved!")
				}else {
					fmt.Println("Save failed!")
				}
			}
		})
	if ch != nil{
		ch <- true
	}
}

func InfoSpite(info string) (director, actor, year string) {
	directorRe, _ := regexp.Compile(`导演:(.*)主演:`)
	director = string(directorRe.Find([]byte(info)))
	director = strings.Trim(director, "主演:")
	director = strings.Trim(director, "导演:")

	actorRe, _ := regexp.Compile(`主演:(.*)`)
	actor = string(actorRe.Find([]byte(info)))
	actor = strings.Trim(actor, "主演:")
	// 年份
	yearRe, _ := regexp.Compile(`(\d+)`)       // 构造的正则表达式
	year = string(yearRe.Find([]byte(info)))   // 使用上式获取的年份
	return
}


func InitDB() (err error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True", USERNAME, PASSWORD, HOST, PORT, DBNAME)
	// 也可以使用MustConnect连接不成功就panic
	db, err = sqlx.Connect("mysql", dsn)
	if err != nil {
		log.Fatal("connect DB failed: ", err)
		return
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	return
}

func CloseDB() {
	_ = db.Close()
}

func InsertData(movieData MovieData) bool {
	sqlStr := "insert into douban_movies(Title, Director,Picture,Actor,Year,Score,Quote) values (?, ?, ?, ?, ?, ?, ?)"
	ret, err := db.Exec(sqlStr, movieData.Title, movieData.Director, movieData.Picture, movieData.Actor, movieData.Year, movieData.Score, movieData.Quote)
	if err != nil {
		fmt.Printf("insert failed, err:%v\n", err)
		return false
	}
	theID, err := ret.LastInsertId() // 新插入数据的id
	if err != nil {
		fmt.Printf("get lastinsert ID failed, err:%v\n", err)
		return false
	}
	fmt.Printf("insert success, the id is %d.\n", theID)
	return true
}




/*
1、解析网页
	1.1 CSS选择器
		"github.com/PuerkitoBio/goquery" 提供了 .NewDocumentFromReader 进行网页解析
	1.2 xpath
		"github.com/antchfx/htmlquery" 提供了 .Parse 进行网页解析
	1.3 正则表达式
		regexp库
 */
