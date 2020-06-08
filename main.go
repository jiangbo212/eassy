package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	_ "github.com/mattn/go-sqlite3"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

/*
	手机抽奖redis更新逻辑, KEYS[1]:手机奖品已发放数量的key,KEYS[2]:手机奖品每天发放数量的key,ARGV[1]:手机奖品最大数量
	local all_count_str = redis.call('exists', KEYS[1])
	local cur_count_str = redis.call('exists', KEYS[2])
	local all_count = tonumber(all_count_str)
	local cur_count = tonumber(cur_count_str)
	if all_count ==0 then redis.call('set', KEYS[1], 0) end
	if cur_count > 0 then return 0 end
	local all_count_value_str = redis.call('get', KEYS[1])
	local all_count_value = tonumber(all_count_value_str)
	local max_count = tonumber(ARGV[1])
	if all_count_value >= max_count
  		then return 0
  		else
    		redis.call('incr', KEYS[1])
    		redis.call('incr', KEYS[2])
    		return 1
  		end
*/
const PhoneLotteryLua = "local all_count_str = redis.call('exists', KEYS[1]) local cur_count_str = redis.call('exists', KEYS[2]) local all_count = tonumber(all_count_str) local cur_count = tonumber(cur_count_str) if all_count ==0 then redis.call('set', KEYS[1], 0) end if cur_count > 0 then return 0 end local all_count_value_str = redis.call('get', KEYS[1]) local all_count_value = tonumber(all_count_value_str) local max_count = tonumber(ARGV[1]) if all_count_value >= max_count then return 0 else redis.call('incr', KEYS[1]) redis.call('incr', KEYS[2]) return 1 end"

/*
	电话卡抽奖redis更新逻辑, KEYS[1]:电话卡奖品已发放数量的key,KEYS[2]:电话卡奖品每个用户获取数量的key,ARGV[1],电话卡奖品最大数量,ARGV[2],电话卡每个用户可获取的最大数量
	local all_count_str = redis.call('exists', KEYS[1])
	local cur_count_str = redis.call('exists', KEYS[2])
	local all_count = tonumber(all_count_str)
	local cur_count = tonumber(cur_count_str)
	local max_count = tonumber(ARGV[1])
	local max_user_count = tonumber(ARGV[2])
	if all_count ==0 then redis.call('set', KEYS[1], 0) end
	if cur_count ==0 then redis.call('set', KEYS[2], 0) end
	local user_count_str = redis.call('get', KEYS[2])
	local user_count = tonumber(user_count_str)
	if user_count >= max_user_count then return 0 end
	local all_count_value_str = redis.call('get', KEYS[1])
	local all_count_value = tonumber(all_count_value_str)
	if all_count_value >= max_count
	  then return 0
	  else
		redis.call('incr', KEYS[1])
		redis.call('incr', KEYS[2])
		return 1
	  end
*/
const CardLotteryLua = "local all_count_str = redis.call('exists', KEYS[1]) local cur_count_str = redis.call('exists', KEYS[2]) local all_count = tonumber(all_count_str) local cur_count = tonumber(cur_count_str) local max_count = tonumber(ARGV[1]) local max_user_count = tonumber(ARGV[2]) if all_count ==0 then redis.call('set', KEYS[1], 0) end if cur_count ==0 then redis.call('set', KEYS[2], 0) end local user_count_str = redis.call('get', KEYS[2]) local user_count = tonumber(user_count_str) if user_count >= max_user_count then return 0 end local all_count_value_str = redis.call('get', KEYS[1]) local all_count_value = tonumber(all_count_value_str) if all_count_value >= max_count then return 0 else redis.call('incr', KEYS[1]) redis.call('incr', KEYS[2]) return 1 end"

/*
	phoneLotteryKey, 手机抽奖的已发放手机奖品的key
	cardLotteryKey, 电话卡抽奖的已发放电话卡奖品的key
*/
const phoneLotteryKey, cardLotteryKey = "phone_lottery_amount", "card_lottery_amount"

/*
	phoneMaxCount, 手机奖品的最大数量
	cardMaxCount，电话卡奖品的最大数量
	cardUserMaxCount, 单个用户可获取到的电话卡奖品的最大数量
*/
const phoneMaxCount, cardMaxCount, cardUserMaxCount = "5", "100", "2"

/*
	captchaQueue, 验证码消息队列
	registerQueue, 注册信息消息队列
*/
const captchaQueue, registerQueue, lotteryQueue = "CAPTCHA_QUEUE", "REGISTER_QUEUE", "LOTTERY_QUEUE"

/*
	手机号正则表达式
*/
const regepPhoneNumber = `^1[3-9]\d{9}$`

/*
	日期格式化字符串
*/
const dateFormat = "20060102"

/*
	sqlite db location
*/
const sqliteDbLocation = "./test.db"

/*
	创建注册信息表
*/
const registerSql = `
	create table main."register"
     (
         id          INTEGER PRIMARY KEY AUTOINCREMENT,
         phone       VARCHAR(11)   NOT NULL,
         content     VARCHAR(1024) NOT NULL,
         create_time DATE          NULL
     )
`

/*
	创建中奖记录表
*/
const lotterySql = `
   create table main."lottery"
	(
		id          INTEGER PRIMARY KEY AUTOINCREMENT,
		phone       VARCHAR(11) NOT NULL,
		result      VARCHAR(2)  NOT NULL,
		cur_day     VARCHAR(8)  NOT NULL,
		create_time DATE        NULL
	)
`

/*
	注册信息表增加手机号作为索引
*/
const registerIndex = "create index register_phone on register(phone)"

/*
	中奖信息表增加手机号作为索引
*/
const lotteryIndex = "create index lottery_phone on lottery(phone)"

var redisClient = redis.NewClient(&redis.Options{
	Addr: "127.0.0.1:6379",
})

/*
	发送验证码
*/
func captchaHandler(w http.ResponseWriter, r *http.Request) {
	var captchaCode = "666666"

	var phoneNumber = r.PostFormValue("phoneNumber")

	result, error := regexp.Match(regepPhoneNumber, []byte(phoneNumber))

	if len(phoneNumber) == 0 || error != nil || !result {
		fmt.Fprint(w, "手机号不合法")
		return
	}

	val, err := redisClient.SetNX(redisClient.Context(), phoneNumber, captchaCode, 60*time.Second).Result()

	if err != nil {
		fmt.Println("connect redis server error", err)
		fmt.Fprint(w, "系统异常")
		return
	}

	if !val {
		fmt.Fprint(w, "请等待60秒后再次获取验证码")
		return
	}

	sendCaptcha(captchaCode)

	fmt.Fprint(w, captchaCode)
}

/*
	验证码消息入队列
*/
func sendCaptcha(captcha string) {
	result := redisClient.LPush(redisClient.Context(), captchaQueue, captcha).Val()
	if result > 0 {
		fmt.Println("验证码消息发送成功", captcha)
	}
}

/*
	用户注册
*/
func registerHandler(w http.ResponseWriter, r *http.Request) {

	var captcha, phoneNumber, content = r.PostFormValue("captcha"), r.PostFormValue("phoneNumber"), r.PostFormValue("content")

	redisCaptcha := redisClient.Get(redisClient.Context(), phoneNumber).Val()

	if len(redisCaptcha) == 0 || captcha != redisCaptcha {
		fmt.Fprint(w, "验证码错误")
		return
	}

	sendRegisterMessage(phoneNumber, content)

	fmt.Fprint(w, "register success")
}

/*
	注册消息入队列
*/
func sendRegisterMessage(phoneNumber string, content string) {
	result := redisClient.LPush(redisClient.Context(), registerQueue, phoneNumber+","+content).Val()
	if result > 0 {
		fmt.Println("注册消息发送成功", phoneNumber, content)
	}
}

/*
	用户抽奖
*/
func lotteryHandler(w http.ResponseWriter, r *http.Request) {

	curDay := time.Now().Format(dateFormat)

	var phoneNumber = r.PostFormValue("phoneNumber")

	var lotteryKey = phoneNumber + "_" + curDay

	lotteryValue, error := redisClient.SetNX(redisClient.Context(), lotteryKey, "1", 24*time.Hour).Result()

	if error != nil {
		fmt.Fprint(w, "系统异常, 请稍后重试")
		return
	}

	// key已经存在, 已经参与过抽奖的直接返回
	if !lotteryValue {
		fmt.Fprint(w, "今天已经参与过抽奖了, 请明天再来")
		return
	}

	result := lotteryResult(phoneNumber)

	var phoneLotteryCurDay, cardLotteryUserNo = "phone_lottery_" + curDay, "card_lottery_" + phoneNumber

	var phoneLotteryKeys = []string{phoneLotteryKey, phoneLotteryCurDay}
	var phoneLotteryArgs = []string{phoneMaxCount}

	var cardLotteryKeys = []string{cardLotteryKey, cardLotteryUserNo}
	var cardLotterArgs = []string{cardMaxCount, cardUserMaxCount}

	switch result {
	case 1:
		val, err := redisClient.Eval(redisClient.Context(), PhoneLotteryLua, phoneLotteryKeys, phoneLotteryArgs).Result()

		if err != nil {
			fmt.Fprint(w, "系统异常, 请稍后重试")
			return
		}

		// 无可发放的手机奖品
		if val == 0 {
			fmt.Fprint(w, "未中奖")
			return
		}

		fmt.Fprint(w, "中奖奖品为手机")
		sendLotteryMessage(phoneNumber, result, time.Now().Format(dateFormat))
		return
	case 2:
		val, err := redisClient.Eval(redisClient.Context(), CardLotteryLua, cardLotteryKeys, cardLotterArgs).Result()

		if err != nil {
			fmt.Fprint(w, "系统异常, 请稍后重试")
			return
		}

		// 无可发放的电话卡奖品
		if val == 0 {
			fmt.Fprint(w, "未中奖")
			return
		}

		fmt.Fprint(w, "中奖奖品为电话卡")
		sendLotteryMessage(phoneNumber, result, time.Now().Format(dateFormat))
		return

	default:
		break
	}

	sendLotteryMessage(phoneNumber, result, time.Now().Format(dateFormat))
	fmt.Fprint(w, "获奖奖品为贴纸")
}

/*
	抽奖成功消息入队列
*/
func sendLotteryMessage(phoneNumber string, rel int, curDay string) {
	result := redisClient.LPush(redisClient.Context(), lotteryQueue, fmt.Sprintf("%s,%d,%s", phoneNumber, rel, curDay)).Val()
	if result > 0 {
		fmt.Println("获奖消息发送成功")
	}
}

/*
	中奖逻辑
*/
func lotteryResult(phoneNumber string) int {

	// 手机
	if strings.Contains(phoneNumber, "239") {
		return 1
	}

	// 电话卡
	if strings.Contains(phoneNumber, "883") {
		return 2
	}

	// 贴纸
	return 0
}

/*
	监听验证码消息并发送
*/
func onCaptChaMessage() {
	for {
		captchaMessage := redisClient.LPop(redisClient.Context(), captchaQueue).Val()

		if len(captchaMessage) > 0 {
			fmt.Println("验证码发送成功", captchaMessage)
		}
	}
}

/*
	监听注册成功消息
*/
func onRegisterMessage() {
	for {
		registerMessage := redisClient.LPop(redisClient.Context(), registerQueue).Val()

		if len(registerMessage) > 0 {
			args := strings.Split(registerMessage, ",")
			key := args[0] + "_register"
			val := redisClient.Get(redisClient.Context(), key).Val()
			if len(val) > 0 {
				fmt.Println("注册消息重复")
			}
			redisClient.Set(redisClient.Context(), key, "1", 30*time.Minute)
			insertData("insert into main.register(phone, content, create_time) values (?,?,CURRENT_TIMESTAMP)", args)
			fmt.Println("注册消息成功落库", registerMessage)
		}
	}
}

/*
	监听抽奖成功消息
*/
func onLotteryMessage() {
	for {
		lotteryMessage := redisClient.LPop(redisClient.Context(), lotteryQueue).Val()

		if len(lotteryMessage) > 0 {
			args := strings.Split(lotteryMessage, ",")
			key := args[0] + "_" + args[1] + "_" + args[2]
			val := redisClient.Get(redisClient.Context(), key).Val()
			if len(val) > 0 {
				fmt.Println("抽奖消息重复")
			}
			redisClient.Set(redisClient.Context(), key, "1", 30*time.Minute)
			insertData("insert into main.lottery(phone, result, cur_day, create_time) VALUES (?,?,?,current_timestamp)", args)
			fmt.Println("抽奖消息成功落库", lotteryMessage)
		}
	}
}

/*
	写入注册数据及中奖数据
*/
func insertData(insertSql string, args []string) {
	db := getConnection()
	stmt, err1 := db.Prepare(insertSql)
	if err1 != nil {
		fmt.Println("写入数据库异常", err1)
	}

	var err = errors.New("init")
	if len(args) == 2 {
		_, err = stmt.Exec(args[0], args[1])
	}

	if len(args) == 3 {
		_, err = stmt.Exec(args[0], args[1], args[2])
	}

	if err != nil {
		fmt.Println("写入数据库异常", err)
	}
	db.Close()
}

/*
	获取数据库连接
*/
func getConnection() *sql.DB {
	db, err := sql.Open("sqlite3", sqliteDbLocation)
	if err != nil {
		fmt.Println("创建sqlite连接异常", err)
		os.Exit(1)
	}
	return db
}

/*
	初始化数据库配置
*/
func initDatabase() {
	os.Create(sqliteDbLocation)

	db := getConnection()

	for _, v := range []string{registerSql, lotterySql, registerIndex, lotteryIndex} {
		_, err := db.Exec(v)
		if err != nil {
			fmt.Println("sql执行异常", v, err)
			os.Exit(1)
		}
	}

	db.Close()

}

func main() {
	go onCaptChaMessage()
	go onRegisterMessage()
	go onLotteryMessage()
	initDatabase()
	mux := http.NewServeMux()
	mux.HandleFunc("/captcha", http.HandlerFunc(captchaHandler))
	mux.Handle("/register", http.HandlerFunc(registerHandler))
	mux.Handle("/lottery", http.HandlerFunc(lotteryHandler))
	http.ListenAndServe(":8000", mux)
}
