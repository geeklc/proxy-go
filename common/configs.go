package common

import (
	"github.com/jinzhu/configor"
	"log"
)

// 定义配置类信息，注意里面变量名称首字母需大写，才可获取配置信息
type Config struct {
	Server struct {
		Port int `default:"8181"`
	}

	// Config 配置文件结构体
	LogFile struct {
		LogDir     string `default:"logs"`
		FileName   string `default:"server.log"`
		LogLevel   string `default:"info"`
		MaxSize    int    `default:"128"`
		MaxBackups int    `default:"180"`
		MaxAge     int    `default:"1"`
		Compress   bool   `default:"false"`
		PrintTag   bool   `default:"false"`
	}

	//mysql配置信息
	Mysql struct {
		UserName string `default:"root"`
		Password string `default:"123456"`
		Host     string `default:"192.168.0.175"`
		Port     int    `default:"4000"`
		DBName   string `default:"xinlian"`
	}
}

var Conf = Config{}

// 读取配置参数
func InitConfig() error {
	//打包使用配置文件路径
	filePath := "./config.yml"
	//本地测试配置文件路径
	//filePath := "src/config1.yml"
	err := configor.Load(&Conf, filePath)
	if err != nil {
		log.Fatal("加载配置文件失败.......，错误信息：", err)
		return err
	}
	return nil
}
