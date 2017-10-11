package main

import (
	"fmt"
	"os"

	"github.com/caarlos0/env"
	"github.com/evadnoob/sqlx-mysql-extended-insert/logging"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	_ "github.com/go-sql-driver/mysql"
)

var log = logging.New()
var DB *sqlx.DB

func main() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

//Connect to database and s3
func Connect() {
	type MysqlConfig struct {
		MysqlPassword string `env:"MYSQL_PASSWORD,required"`
		MysqlUser     string `env:"MYSQL_USER,required" envDefault:"root"`
		MysqlHost     string `env:"MYSQL_HOST,required" envDefault:"mysql-56"`
		MysqlPort     string `env:"MYSQL_PORT" envDefault:"3306"`
		MysqlDatabase string `env:"MYSQL_DATABASE,required"`
	}

	c := MysqlConfig{}
	if err := env.Parse(&c); err != nil {
		panic(err)
	}

	var err error
	DB, err = sqlx.Open("mysql",
		c.MysqlUser+":"+c.MysqlPassword+"@tcp("+c.MysqlHost+":3306)/"+c.MysqlDatabase+"?parseTime=true&interpolateParams=true")

	if err != nil {
		log.Errorf("error opening database connection %s", err)
		panic(err)
	}

	log.Infof("DB: %+v", DB)
	err = DB.Ping()
	if err != nil {
		log.Errorf("error %s", err)
		panic(err)
	}
}

var RootCmd = &cobra.Command{
	Use:   "insert",
	Short: "",
	Long:  ``,
}

var setupCmd = &cobra.Command{
	Use:   "setup",
	Short: "",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {

		Connect()
		result, err := DB.Exec(`create table t1(c1 int unsigned not null auto_increment primary key,
       c2 binary(16), 
       c3 varchar(100));`)

		if err != nil {
			panic(err)
		}
		log.Infof("created table %v", result)

		return nil
	},
}

var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {

		Connect()
		result, err := DB.Exec(`drop table if exists t1;`)
		if err != nil {
			panic(err)
		}
		log.Infof("drop table %v", result)

		return nil
	},
}

var test1Cmd = &cobra.Command{
	Use:   "test1",
	Short: "",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {

		Connect()
		result, err := DB.Exec(`insert into t1(c2, c3)  values(unhex(replace(uuid(), '-', '')), 'test1'), (unhex(replace(uuid(), '-', '')), 'test2')`)
		if err != nil {
			panic(err)
		}

		log.Infof("insert completed %v", result)
		return nil

	},
}

var test2Cmd = &cobra.Command{
	Use:   "test2",
	Short: "",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {

		Connect()

		for i := 0; i < 1000; i++ {
			result := DB.MustExec(`insert into t1(c2, c3)  values(unhex(replace(uuid(), '-', '')), 'test1'), (unhex(replace(uuid(), '-', '')), 'test2')`)
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				log.Warnf("error getting rows affected: %v", err)
			}

			lastInsertId, err := result.LastInsertId()
			if err != nil {
				log.Warnf("error getting lastInsertId : %v", err)
			}

			log.Infof("inserted rowsAffected: %d, id: %d", rowsAffected, lastInsertId)
		}

		x := make(chan interface{}, 10)
		go func() {
			for {
				select {
				case blah := <-x:
					log.Infof("blah: %+v, %T, %d", blah, blah, blah)
				default:
					break
				}
			}

		}()

		rows, err := DB.Query(`select * from t1`)
		if err != nil {
			panic(err)
		}

		defer rows.Close()

		for rows.Next() {
			var id interface{}
			var uuid []byte
			var c3 string

			err = rows.Scan(&id, &uuid, &c3)
			if err != nil {
				panic(err)
			}
			x <- id
		}
	},
}

func init() {
	RootCmd.AddCommand(setupCmd)
	RootCmd.AddCommand(cleanupCmd)
	RootCmd.AddCommand(test1Cmd)
	RootCmd.AddCommand(test2Cmd)

	viper.RegisterAlias("reset", "cleanup")
}
