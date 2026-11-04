// main version 1.0.0
// use my_kv_engine::storage::{StorageEngine, MemoryStorage, DiskStorage};

// fn main() -> my_kv_engine::error::Result<()> {
//     let mut memstorage = MemoryStorage::new();
//     let mut diskstorage = DiskStorage::new("data.json")?;

//     diskstorage.set("language", "rust")?;
//     let value = diskstorage.get("language")?;
//     println!("Got value: {:?}", value);

//     diskstorage.set("language", "rust programming")?;
//     let value = diskstorage.get("language")?;
//     println!("Got updated value: {:?}", value);

//     diskstorage.set("developer", "tenerife")?;
//     let value = diskstorage.get("developer")?;
//     println!("Got value: {:?}", value);

//     diskstorage.set("dev", "blockchain")?;
//     let value = diskstorage.get("dev")?;
//     println!("Got value: {:?}", value);

//     diskstorage.remove("dev")?;
//     let value = diskstorage.get("dev")?;
//     println!("Got value after removal: {:?}", value);

//     memstorage.set("key1", "value1")?;
//     let value = memstorage.get("key1")?;
//     println!("Got value: {:?}", value);

//     memstorage.remove("key1")?;
//     let value = memstorage.get("key1")?;
//     println!("Got value after removal: {:?}", value);

//     memstorage.remove("key1")?; // This will return an error since the key has been removed
//     Ok(())
// }


// main version 2.0.0
// CLI version
// use my_kv_engine::storage::{StorageEngine, MemoryStorage, DiskStorage};
// use std::env;

// fn main() -> my_kv_engine::error::Result<()> {
//     let args: Vec<String> = env::args().collect();
//     if args.len() < 2 {
//         // args[0] is the program name, so we need at least one more argument
//         println!("Usage: Cargo run -- <set|get|rm> [args]");
//         return Ok(());
//     }

//     let mut store = DiskStorage::new("data.json")?;

//     let command = args[1].as_str();// .as_str() converts the String to a &str for easier comparison
//     // rust没有str类型，只有&str类型，字符串切片&str是对字符串的引用，而String是一个拥有所有权的字符串类型。
//     // as_str()方法将String转换为&str，以便在匹配时使用。
//     match command {
//         "set" => {
//             if args.len() != 4 {
//                 println!("Usage: Cargo run -- set <key> <value>");
//             } else {
//                 // 这里不能够将args[2]和args[3]直接与变量绑定，
//                 // 因为args是Vec<String>，rust不允许Vector中出现空洞
//                 let key = &args[2];
//                 let value = &args[3];
//                 store.set(key, value)?;
//                 println!("Set key: {}, value: {}", key, value);
//             }
//         }

//         "get" => {
//             if args.len() != 3 {
//                 println!("Usage: Cargo run -- get <key>"); 
//             } else {
//                 let key = &args[2];
//                 match store.get(key)? {
//                     Some(value) => println!("{}", value),
//                     None => println!("Key not found")
//                 }
//             }
//         }

//         "rm" => {
//             if args.len() != 3 {
//                 println!("Usage: Cargo run -- rm <key>");
//             } else {
//                 let key = &args[2];
//                 println!("Removed key: {}", key);
//                 store.remove(key)?;
//             }
//         }

//         _ => {
//             println!("Unknown command: {}", command);
//             println!("Usage: Cargo run -- <set|get|rm> [args]");
//         }
//     }

//     Ok(())
// }



// main version 3.0.0
// memory storage multi-threaded test

// use my_kv_engine::storage::{StorageEngine, MemoryStorage};
// use std::sync::Arc;
// use std::thread;

// fn main() -> my_kv_engine::error::Result<()> {
//     // 1. 初始化引擎，并用 Arc 包裹起来以供多线程共享
//     let store = Arc::new(MemoryStorage::new());
//     let mut handles = vec![];

//     // 2. 开启 10 个线程，同时并发写入！
//     for i in 0..10 {
//         let store_clone = Arc::clone(&store); // 每个线程拿到一个 Arc 克隆
//         let handle = thread::spawn(move || {
//             let key = format!("key_{}", i);
//             let value = format!("value_{}", i);
//             // 因为 trait 变成了 &self，多线程下直接调用 set 不再报错！
//             store_clone.set(key, value).unwrap(); 
//         });
//         handles.push(handle);
//     }

//     // 3. 等待所有线程执行完毕
//     for handle in handles {
//         handle.join().unwrap();
//     }

//     // 4. 主线程去读取刚才其它线程写入的数据，验证是否成功
//     for i in 0..10 {
//         let key = format!("key_{}", i);
//         let val = store.get(&key)?;
//         println!("Got: {}: {:?}", key, val);
//     }

//     Ok(())
// }




// use my_kv_engine::storage::{StorageEngine, DiskStorage};
// use std::sync::Arc;
// use std::thread;

// fn main() -> my_kv_engine::error::Result<()> {
//     // 1. 初始化持久化引擎，用 Arc 包裹
//     let store = Arc::new(DiskStorage::new("data.json")?);
//     let mut handles = vec![];

//     println!("开始百线程并发轰炸...");

//     // 2. 开启 100 个线程，疯狂并发写入
//     for i in 0..100 {
//         let store_clone = Arc::clone(&store);
//         let handle = thread::spawn(move || {
//             let key = format!("user_{}", i);
//             let value = format!("tenerife_{}", i);
//             // 并发写入，全靠 RwLock 保证不会把文件写碎！
//             store_clone.set(key, value).unwrap();
//         });
//         handles.push(handle);
//     }

//     // 3. 等待所有轰炸结束
//     for handle in handles {
//         handle.join().unwrap();
//     }

//     println!("写入完毕，进行抽查验证：");
//     let val = store.get("user_50")?;
//     println!("Got user_50: {:?}", val);

//     println!("去看看你的 data.json 吧，里面有 100 条数据！");
//     Ok(())
// }

use my_kv_engine::storage::{StorageEngine, DiskStorage};
use std::sync::Arc;
// use std::thread;

#[tokio::main]
async fn main() -> my_kv_engine::error::Result<()> {
    // 1. 初始化持久化引擎，用 Arc 包裹
    let store = Arc::new(DiskStorage::new("data.json").await?);
    let mut handles = vec![];

    println!("开始百线程并发轰炸...");

    // 2. 开启 100 个线程，疯狂并发写入
    for i in 0..100 {
        let store_clone = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            let key = format!("user_{}", i);
            let value = format!("tenerife_{}", i);
            // 并发写入，全靠 RwLock 保证不会把文件写碎！
            store_clone.set(key, value).await.unwrap();
        });
        handles.push(handle);
    }

    // 3. 等待所有轰炸结束
    for handle in handles {
        handle.await.unwrap();
    }

    println!("写入完毕，进行抽查验证：");
    let val = store.get("user_50").await?;
    println!("Got user_50: {:?}", val);

    println!("去看看你的 data.json 吧，里面有 100 条数据！");
    Ok(())
}