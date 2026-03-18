package sql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

var MyDB *sql.DB

// 初始化操作数据库连接
func InitFromDB() error {
	connectDB, err := initConnectFor()
	if err != nil {
		return err
	}
	MyDB = connectDB
	return nil
}

// 初始化数据库文件
func initConnectFor() (*sql.DB, error) {
	//加载配置文件
	//conf := common.Conf
	var userName, password, host, port, dbName = "root", "123456", "192.168.0.175", 4000, "xinlian"
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Local", userName, password, host, port, dbName)
	//注意，这里不会校验密码是否正确
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, err
}
