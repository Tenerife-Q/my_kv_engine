use tokio::net::{TcpListener, TcpStream};
use std::sync::Arc;
use my_kv_engine::storage::{StorageEngine, DiskStorage};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::io::{AsyncBufReadExt, BufReader};

// handle_client version 1.0
// async fn handle_client(mut stream: TcpStream, engine: Arc<DiskStorage>) {
//     println!("New connection established!");

//     loop {
//         let mut buffer = [0u8; 1024];
        
//         // read 会返回 Result<usize>，必须用 match 解包处理
//         match stream.read(&mut buffer).await {
//             Ok(0) => {
//                 // TODO: 读到 0 字节代表什么？在这里写下你的处理逻辑
//                 println!("Client disconnected.");
//                 break; // 跳出循环
//             }
//             Ok(n) => {
//                 // 1. 将前 n 个字节转为字符串，并去掉末尾的 \r\n
//                 let request_str = String::from_utf8_lossy(&buffer[..n]);
//                 let text = request_str.trim();

//                 // 如果用户只敲了个回车，直接跳过当前循环，重新接水
//                 if text.is_empty() {
//                     continue;
//                 }

//                 // 2. 将命令按空格分词，变成 Vec<&str>
//                 // 比如 "set user1 123" 会变成 ["set", "user1", "123"]
//                 let parts: Vec<&str> = text.split_whitespace().collect();

//                 // 3. 准备一个要返回给客户端的字符串
//                 // let mut String::new();
//                 // 不要像上面这样写，rust是一门一切皆为表达式的语言
//                 // 下面是match语句，能够直接返回值
//                 // 绑定给 response 变量

//                 // 4. 祭出 Rust 的切片模式匹配（Slice Pattern Matching）
//                 // 我们把 Vec 转换成切片 &[&str] 来进行高级匹配
//                 let response = match parts.as_slice() {
//                     ["set", key, value] => {
//                         // TODO: 调用 engine 的 set 方法！
//                         // 注意：engine.set 返回 Result，你需要用 match 判断是否成功。
//                         // 成功就给 response 赋值 "+OK\r\n"
//                         // 失败就给 response 赋值 format!("-ERR {}\r\n", e)
//                         // *key 和 *value 是解引用，拿到 &str 的值
//                         // 因为as_slice()返回的是 &[&str]，切片匹配需要借用
//                         // 所以key value类型是&&str，调用set方法需要传入&str类型，所以需要解引用一次。
//                         match engine.set(*key, *value).await {
//                             Ok(_) => "+OK\r\n".to_string(),
//                             Err(e) => format!("-ERR {}\r\n", e),
//                         }
//                     },
//                     ["get", key] => {
//                         // TODO: 调用 engine 的 get 方法！
//                         // 注意 get 返回 Result<Option<String>>。
//                         // 如果成功且有值，返回对应的值（比如加个前缀 "$"）；
//                         // 如果成功但没值（None），返回 "$-1\r\n" 代表不存在；
//                         // 如果底层报错，返回 "-ERR ...\r\n"
//                         // 这里不加*也行，因为get方法的参数是&str类型，key本身就是&&str类型，
//                         // Rust使用Deref coercion会自动解引用，所以可以直接传入key。
//                         // 但是前后两个就不行，因为set和remove方法的参数是impl Into<String>类型，传入&&str会报错。
//                         // 因为范型严格匹配
//                         match engine.get(*key).await {
//                             Ok(Some(val)) => format!("${}\r\n", val),
//                             Ok(None) => "$-1\r\n".to_string(),
//                             Err(e) => format!("-ERR {}\r\n", e),
//                         }
//                     },
//                     ["rm", key] => {
//                         // TODO: 调用 engine 的 remove 方法，自己仿照上面的写法完成！
//                         match engine.remove(*key).await {
//                             Ok(_) => "+OK\r\n".to_string(),
//                             Err(e) => format!("-ERR {}\r\n", e),
//                         }
//                     },
                     
//                     // 匹配所有不认识的命令，或者参数数量不对的情况（比如只敲了 "set"）
//                     _ => "-ERR Unknown command or wrong arguments\r\n".to_string(),
        
//                 };

//                 // 5. 把组装好的字符串写回 TCP 管道
//                 // 使用 write_all 确保所有字节都被写进去
//                 stream.write_all(response.as_bytes()).await.expect("Write failed");
//             }
//             Err(e) => {
//                 // TODO: 如果网络读取报错，打印错误并退出当前循环
//                 eprintln!("Failed to read: {}", e);
//                 break; // 跳出循环
//             }
//         }
//     }
// }

async fn handle_client(stream: TcpStream, engine: Arc<DiskStorage>) {
    println!("New connection established!");

    // 1. 将 TCP 流拆分为独立的读半部和写半部
    let (reader, mut writer) = stream.into_split();

    // 2. 将读端包装进带有缓冲区的 BufReader
    let mut buf_reader = BufReader::new(reader);
    
    // 3. 准备一个可复用的字符串缓冲区
    let mut line = String::new();

    loop {
        // 必须清空缓冲区，因为 read_line 是追加行为
        line.clear();

        // 4. read_line 会自动处理底层的系统调用、半包和粘包。
        // 它会把字节暂存到内存，直到遇到 '\n' 才返回完整的一行。
        match buf_reader.read_line(&mut line).await {

            Ok(0) => {
                println!("Client disconnected.");
                break;
            }

            Ok(_) => {
                let text = line.trim();
                if text.is_empty() {
                    continue;
                }

                // 分词
                let parts: Vec<&str> = text.split_whitespace().collect();

                // 5. 组装响应 (将你原有的 match parts.as_slice() 逻辑搬过来)
                let response = match parts.as_slice() {
                    ["set", key, value] => {
                        match engine.set(*key, *value).await {
                            Ok(_) => "+OK\r\n".to_string(),
                            Err(e) => format!("-ERR {}\r\n", e),
                        }
                    }
                    ["get", key] => {
                        match engine.get(*key).await {
                            Ok(Some(value)) => format!("{}\r\n", value),
                            Ok(None) => "-ERR Key not found\r\n".to_string(),
                            Err(e) => format!("-ERR {}\r\n", e),
                        }
                    }
                    ["rm", key] => {
                        match engine.remove(*key).await {
                            Ok(_) => "+OK\r\n".to_string(),
                            Err(e) => format!("-ERR {}\r\n", e),
                        }
                    }

                    ["compact"] => {
                        match engine.compact().await {
                            Ok(_) => "+OK\r\n".to_string(),
                            Err(e) => format!("-ERR {}\r\n", e),
                        }
                    }

                    _ => "-ERR Unknown command\r\n".to_string(),
                };

                // 6. 使用独立的写端写回数据
                writer.write_all(response.as_bytes()).await.expect("Write failed");
            }

            Err(e) => {
                eprintln!("Failed to read: {}", e);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() -> () {
    let engine = Arc::new(DiskStorage::new("data.json").await.expect("Failed to initialize storage engine"));
    let listener = TcpListener::bind("127.0.0.1:6379").await.expect("Failed to bind to address");
    println!("KV Server listening on 127.0.0.1:6379");

    /*
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let engine_clone = Arc::clone(&engine);
                std::thread::spawn(move || {
                    handle_client(stream, engine_clone);
                });
            }
            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
            }
        }
    }
    */

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
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