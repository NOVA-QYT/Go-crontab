package master

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"

	//"github.com/mongodb/mongo-go-driver/mongo/findopt"
	"github.com/owenliang/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

// mongodb日志管理
type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)

	// 建立mongodb连接
	//if client, err = mongo.Connect(
	//	context.TODO(),
	//	G_config.MongodbUri,
	//	clientopt.ConnectTimeout(time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond)); err != nil {
	//	return
	//}

	// 建立mongodb连接
	ctx, cancel := context.WithTimeout(context.TODO(), time.Duration(G_config.MongodbConnectTimeout)*time.Second)
	defer cancel()
	if client, err = mongo.Connect(ctx, options.Client().ApplyURI(G_config.MongodbUri)); err != nil {
		//fmt.Println(err)
		return
	}

	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return
}

// 查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip int, limit int) (logArr []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor  mongo.Cursor
		jobLog  *common.JobLog
	)

	// len(logArr)
	logArr = make([]*common.JobLog, 0)

	// 过滤条件
	filter = &common.JobLogFilter{JobName: name}

	// 按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	findopt := options.Find()
	findopt.Sort(logSort)
	//findopt.SetSkip(int64(skip))
	//findopt.SetLimit(int64(limit))

	//options.Find()
	////查询
	//if cursor, err = logMgr.logCollection.Find(context.TODO(), filter, findopt); err != nil {
	//	return
	//}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(G_config.MongodbConnectTimeout)*time.Second)
	defer cancel()

	cursor, err = logMgr.logCollection.Find(ctx, filter, findopt)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(ctx)

	if err != nil {
		return
	}

	//// 4, 按照jobName字段过滤, 想找出jobName=job10, 找出5条xw
	//cond = &FindByJobName{JobName: "job10"} // {"jobName": "job10"}
	//
	//findOptions := options.Find()
	//findOptions.SetSkip(0)
	//findOptions.SetLimit(2)

	//cursor, err = collection.Find(ctx, bson.D{{"jobName", "job100"}}, findOptions)
	//
	//// 5, 查询（过滤 +翻页参数）
	//if cursor, err = collection.Find(ctx, cond, findOptions); err != nil {
	//	fmt.Println(err)
	//	return
	//}

	// 延迟释放游标
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}

		// 反序列化BSON
		if err = cursor.Decode(jobLog); err != nil {
			continue // 有日志不合法
		}

		logArr = append(logArr, jobLog)
	}
	return
}
