// grpc服务连接池

package gpool

import (
	"errors"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type (
	//Conn 连接信息结构体
	Conn struct {
		conn *grpc.ClientConn
		//连接时间
		createTime time.Time
	}

	//ConnPool 连接池
	ConnPool struct {
		serverURL string
		//互斥锁，保证资源安全
		mu sync.Mutex
		//通道，保存所有连接资源
		conns chan *Conn
		//判断池是否关闭
		closed bool
		//连接超时时间
		lifeTime time.Duration
	}
)

//NewConnPool 创建一个连接资源池
func NewConnPool(cap int, serverURL string, connLifeTime time.Duration) (*ConnPool, error) {
	if cap <= 0 {
		return nil, errors.New("cap不能小于0")
	}
	if connLifeTime <= 0 {
		return nil, errors.New("connLifeTime不能小于0")
	}
	cp := &ConnPool{
		serverURL: serverURL,
		mu:        sync.Mutex{},
		conns:     make(chan *Conn, cap),
		closed:    false,
		lifeTime:  connLifeTime,
	}
	for i := 0; i < cap; i++ {
		//通过工厂方法创建连接资源
		connRes, err := cp.factory()
		if err != nil {
			cp.Close()
			return nil, errors.New("factory出错")
		}
		//将连接资源插入通道中
		cp.conns <- &Conn{conn: connRes, createTime: time.Now()}
	}
	return cp, nil
}

//Get 获取连接资源
func (cp *ConnPool) Get() (*grpc.ClientConn, error) {
	if cp.closed {
		return nil, errors.New("连接池已关闭")
	}

	for {
		select {
		//从通道中获取连接资源
		case connRes, ok := <-cp.conns:
			{
				if !ok {
					return nil, errors.New("连接池已关闭")
				}
				log.Println("grpc connection state:", connRes.conn.GetState())

				// 判断连接状态
				if connRes.conn.GetState() != connectivity.Ready { //判断连接状态
					connRes.conn.Close()
					continue
				}
				// 连接的创建时间过长,则关闭该连接, 并重新获取
				if time.Now().Sub(connRes.createTime) > cp.lifeTime {
					connRes.conn.Close()
					continue
				}
				return connRes.conn, nil
			}
		default:
			{
				//如果无法从通道中获取资源，则重新创建一个资源返回
				log.Println("All connections are closed, will create new connection.")
				connRes, err := cp.factory()
				if err != nil {
					return nil, err
				}
				return connRes, nil
			}
		}
	}
}

//Put 连接资源放回池中
func (cp *ConnPool) Put(conn *grpc.ClientConn) error {
	if cp.closed {
		return errors.New("连接池已关闭")
	}

	select {
	//向通道中加入连接资源
	case cp.conns <- &Conn{conn: conn, createTime: time.Now()}:
		return nil
	default:
		{
			//如果无法加入，则关闭连接
			conn.Close()
			return errors.New("连接池已满")
		}
	}
}

//Close 关闭连接池
func (cp *ConnPool) Close() {
	if cp.closed {
		return
	}
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.closed = true
	//关闭通道
	close(cp.conns)
	//循环关闭通道中的连接
	for conn := range cp.conns {
		conn.conn.Close()
	}
}

//返回池中通道的长度
func (cp *ConnPool) len() int {
	return len(cp.conns)
}

func (cp *ConnPool) factory() (*grpc.ClientConn, error) {
	// Set up a connection to the server.
	// fmt.Println("connect to grpc server:", cp.serverURL)
	// ctx, cncl := context.WithTimeout(context.Background(), 5*time.Second)
	// defer cncl()
	// conn, err := grpc.DialContext(ctx, cp.serverURL, grpc.WithInsecure(), grpc.WithBlock()) // 设置阻塞, 5s超时
	conn, err := grpc.Dial(cp.serverURL, grpc.WithInsecure())
	if err != nil {
		log.Printf("connect %s error: %v", cp.serverURL, err)
		return nil, err
	}
	return conn, nil
}
