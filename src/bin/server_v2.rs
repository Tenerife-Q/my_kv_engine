use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
// 注意：必须引入这两个 trait，才能对 Framed 调用 .next() 和 .send()
use futures::sink::SinkExt;
use tokio_stream::StreamExt; 

use my_kv_engine::storage::{StorageEngine, DiskStorage};
use my_kv_engine::codec::CommandCodec;
use my_kv_engine::cmd::Command;

async fn handle_client(stream: TcpStream, engine: Arc<DiskStorage>) {
    // 1. 将原生的 TCP 流升级为按“帧”处理的流
    // 注意：这里不需要再用 into_split 拆分读写端了！
    // Framed 同时实现了 Stream (负责产出 Command) 和 Sink (负责接收 String)
    let mut framed = Framed::new(stream, CommandCodec);

    // 2. 循环处理请求
    // next() 会自动调用底层的 decode，直到解析出一个完整的 Command 才会返回。
    // 如果客户端断开，next() 返回 None。
    while let Some(request) = framed.next().await {
        match request {
            Ok(cmd) => {
                // 此时你拿到的 cmd 已经是强类型的 Command 枚举了！
                let response = match cmd {
                    Command::Set { key, value } => {
                    // 拦截 Result，成功返回 "+OK"，失败返回 "-ERR ..."
                        match engine.set(&key, &value).await {
                            Ok(_) => "+OK".to_string(),
                            Err(e) => format!("-ERR {}", e),
                        }
                    }
                    Command::Get { key } => {
                        match engine.get(&key).await {
                            Ok(Some(val)) => val, // 成功且有值
                            Ok(None) => "-ERR Key not found".to_string(), // 成功但没值
                            Err(e) => format!("-ERR {}", e), // 底层报错
                        }
                    }
                    // TODO: 自己把 Remove 和 Compact 的逻辑也改成类似 Set 的 match 处理。
                    Command::Remove { key } => {
                        match engine.remove(&key).await {
                            Ok(_) => "+OK".to_string(),
                            Err(e) => format!("-ERR {}", e),
                        }
                    }
                    Command::Compact => {
                        match engine.compact().await {
                            Ok(_) => "+OK".to_string(),
                            Err(e) => format!("-ERR {}", e),
                        }
                    }
                };

                // 3. 将响应写回客户端
                // send() 会自动调用底层的 encode，拼凑好长度前缀后发往网卡
                if let Err(e) = framed.send(response).await {
                    eprintln!("Failed to send response: {}", e);
                    break;
                }
            }
            Err(e) => {
                eprintln!("Failed to decode frame: {}", e);
                break;
            }
        }
    }
    println!("Client connection closed.");
}

#[tokio::main]
async fn main() {
    // TODO: 照搬你之前的 main 函数逻辑，启动监听并 spawn 处理 handle_client
    let engine = Arc::new(DiskStorage::new("my_kv_engine.db").await.unwrap());
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Server is listening on 127.0.0.1:6379");

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("Accepted connection from {}", addr);
                let engine_clone = Arc::clone(&engine);
                tokio::spawn(async move {
                    handle_client(stream, engine_clone).await;
                });
            }

            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
            }
        }
    }
}