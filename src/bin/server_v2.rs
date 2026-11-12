// use std::sync::Arc;
// use tokio::net::{TcpListener, TcpStream};
// use tokio_util::codec::Framed;
// // 注意：必须引入这两个 trait，才能对 Framed 调用 .next() 和 .send()
// use futures::sink::SinkExt;
// use tokio_stream::StreamExt; 

// use my_kv_engine::storage::{StorageEngine, DiskStorage};
// use my_kv_engine::codec::CommandCodec;
// use my_kv_engine::cmd::Command;

// async fn handle_client(stream: TcpStream, engine: Arc<DiskStorage>) {
//     // 1. 将原生的 TCP 流升级为按“帧”处理的流
//     // 注意：这里不需要再用 into_split 拆分读写端了！
//     // Framed 同时实现了 Stream (负责产出 Command) 和 Sink (负责接收 String)
//     let mut framed = Framed::new(stream, CommandCodec);

//     // 2. 循环处理请求
//     // next() 会自动调用底层的 decode，直到解析出一个完整的 Command 才会返回。
//     // 如果客户端断开，next() 返回 None。
//     while let Some(request) = framed.next().await {
//         match request {
//             Ok(cmd) => {
//                 // 此时你拿到的 cmd 已经是强类型的 Command 枚举了！
//                 let response = match cmd {
//                     Command::Set { key, value } => {
//                     // 拦截 Result，成功返回 "+OK"，失败返回 "-ERR ..."
//                         match engine.set(&key, &value).await {
//                             Ok(_) => "+OK".to_string(),
//                             Err(e) => format!("-ERR {}", e),
//                         }
//                     }
//                     Command::Get { key } => {
//                         match engine.get(&key).await {
//                             Ok(Some(val)) => val, // 成功且有值
//                             Ok(None) => "-ERR Key not found".to_string(), // 成功但没值
//                             Err(e) => format!("-ERR {}", e), // 底层报错
//                         }
//                     }
//                     // TODO: 自己把 Remove 和 Compact 的逻辑也改成类似 Set 的 match 处理。
//                     Command::Remove { key } => {
//                         match engine.remove(&key).await {
//                             Ok(_) => "+OK".to_string(),
//                             Err(e) => format!("-ERR {}", e),
//                         }
//                     }
//                     Command::Compact => {
//                         match engine.compact().await {
//                             Ok(_) => "+OK".to_string(),
//                             Err(e) => format!("-ERR {}", e),
//                         }
//                     }
//                 };

//                 // 3. 将响应写回客户端
//                 // send() 会自动调用底层的 encode，拼凑好长度前缀后发往网卡
//                 if let Err(e) = framed.send(response).await {
//                     eprintln!("Failed to send response: {}", e);
//                     break;
//                 }
//             }
//             Err(e) => {
//                 eprintln!("Failed to decode frame: {}", e);
//                 break;
//             }
//         }
//     }
//     println!("Client connection closed.");
// }

// #[tokio::main]
// async fn main() {
//     // TODO: 照搬你之前的 main 函数逻辑，启动监听并 spawn 处理 handle_client
//     let engine = Arc::new(DiskStorage::new("my_kv_engine.db").await.unwrap());
//     let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
//     println!("Server is listening on 127.0.0.1:6379");

//     loop {
//         match listener.accept().await {
//             Ok((stream, addr)) => {
//                 println!("Accepted connection from {}", addr);
//                 let engine_clone = Arc::clone(&engine);
//                 tokio::spawn(async move {
//                     handle_client(stream, engine_clone).await;
//                 });
//             }

//             Err(e) => {
//                 eprintln!("Failed to accept connection: {}", e);
//             }
//         }
//     }
// }


use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use futures::sink::SinkExt;
use tokio_stream::StreamExt;
use clap::Parser; // 引入 clap 的核心 Trait

use my_kv_engine::storage::{StorageEngine, DiskStorage};
use my_kv_engine::codec::CommandCodec;
use my_kv_engine::cmd::Command;

// 1. 定义配置结构体。
// 为什么放在这里？因为它是 Server 专属的入口配置，不需要污染 lib.rs。
#[derive(Parser, Debug)]
#[command(name = "my-kv-server", version = "2.0", about = "Bitcask KV Storage Server")]
struct Args {
    // 监听端口，默认为 6379
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    // 数据库文件路径，默认为当前目录的 data.log
    #[arg(short = 'd', long = "db", default_value = "data.log")]
    db_path: String,
}

// (你的 handle_client 函数完全保持不变，不要动它)
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
    // 2. 在 main 函数启动的第一行，立刻接管终端参数！
    // 它是阻塞的同步调用。如果参数不对，程序在这里直接退出。
    let args = Args::parse();

    // 3. 将硬编码的路径替换为 args.db_path
    let engine = Arc::new(DiskStorage::new(args.db_path).await.unwrap());
    
    // 4. 将硬编码的端口替换为 args.port
    let addr = format!("127.0.0.1:{}", args.port);
    let listener = TcpListener::bind(&addr).await.unwrap();
    
    // 打印出来，让你自己看到参数是否生效
    println!("Server is listening on {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, client_addr)) => {
                println!("Accepted connection from {}", client_addr);
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