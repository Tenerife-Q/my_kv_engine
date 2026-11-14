use crate::error::{KvError, Result};
use std::collections::HashMap;
use std::sync::Arc;

use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, RwLock}; // 注意：引入了 Tokio 的 Mutex 用来锁文件句柄
use tokio::io::{AsyncBufReadExt, BufReader};


// trait version 1.0
// pub trait StorageEngine {
//     fn set(&mut self, key: String, value: String) -> Result<()>;
//     fn get(&self, key: String) -> Result<Option<String>>;
//     fn remove(&mut self, key: String) -> Result<()>;
// }


// trait version 2.0
// pub trait StorageEngine {
//     fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> Result<()>;
//     fn get(&self, key: &str) -> Result<Option<String>>; // 思考一下：为什么 get 只需要 &str？
//     fn remove(&mut self, key: impl Into<String>) -> Result<()>;
// }

// trait version 3.0
// pub trait StorageEngine {
//     fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()>;
//     fn get(&self, key: &str) -> Result<Option<String>>;
//     fn remove(&self, key: impl Into<String>) -> Result<()>;
// }

#[allow(async_fn_in_trait)]
pub trait StorageEngine {
    async fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<String>>;
    async fn remove(&self, key: impl Into<String>) -> Result<()>;
}

#[derive(Clone)]
pub struct MemoryStorage {
    // Arc允许多个线程安全地共享内存，RwLock允许多个读者或一个写者访问数据。
    map: Arc<RwLock<HashMap<String, String>>>,
}
// DiskStorage struct version 1.0
// pub struct DiskStorage {
//     map: Arc<RwLock<HashMap<String, String>>>,
//     file_path: String,
//     wal_file: Arc<Mutex<File>>,
// }

use std::os::unix::fs::FileExt; // 引入 read_at
// ... 其他你需要的引入

// 1. 定义内存索引对象
#[derive(Debug, Clone)]
pub struct CommandPos {
    pub offset: u64,
    pub length: u32,
}

// 2. 重构存储引擎结构体
pub struct DiskStorage {
    // 内存里现在只存 CommandPos，体积缩减成千上万倍！
    index: Arc<RwLock<HashMap<String, CommandPos>>>,
    
    // 写入句柄（必须是独占锁）。
    // 为什么把 u64 塞进去？因为我们需要精确追踪当前写入的偏移量（游标）。
    // 每次写入后，游标 += 写入的字节数。
    wal_writer: Arc<Mutex<(tokio::fs::File, u64)>>,
    
    // 读取句柄。
    // 注意看类型：这是标准库的同步 File！并且没有任何 Mutex 锁！
    // 依靠 FileExt::read_at，它可以在多线程下极速并发安全读取。
    wal_reader: Arc<std::fs::File>, 
    
    file_path: String,
}

// new() for DiskStorage version 1.0
// impl DiskStorage {
//     pub async fn new(path: impl Into<String>) -> Result<Self> {
//         let file_path = path.into();
//         let map = if std::path::Path::new(&file_path).exists() {
//             // let content = std::fs::read_to_string(&file_path)?;
//             let content = tokio::fs::read_to_string(&file_path).await?;
//             serde_json::from_str(&content)?
//         } else {
//             HashMap::new()
//         };
        
//         Ok(Self {
//             map: Arc::new(RwLock::new(map)),
//             file_path,
//         })
//     }
// }


// new() for DiskStorage version 2.0
/*
impl DiskStorage {
    pub async fn new(path: impl Into<String>) -> Result<Self> {
        let file_path = path.into();
        let mut map = HashMap::new();

        // 1. 重放阶段：尝试以纯读模式打开文件
        // 查阅文档关键词：tokio::fs::File::open
        if let Ok(file) = tokio::fs::File::open(&file_path).await {
            let reader = BufReader::new(file);
            let mut lines = reader.lines(); // 必须引入 AsyncBufReadExt trait 才能用 lines()

            // 逐行读取文件
            while let Some(line) = lines.next_line().await? {
                // todo: 1. 按 '\t' 分割当前行，收集为 Vec<&str>
                let parts: Vec<&str> = line.split('\t').collect();

                // todo: 2. 模式匹配切片 (回忆 server.rs 里的写法)
                match parts.as_slice() {
                    ["S", key, value] => {
                        // 执行 map.insert
                        // 注意：你需要将 key 和 value 转换为 String 再存入，因为 map 的类型是 HashMap<String, String>
                        map.insert(key.to_string(), value.to_string());
                    },
                    ["D", key] => {
                        // 执行 map.remove
                        map.remove(*key);
                    },
                    _ => {
                        // 遇到格式不对的行，直接跳过或者记录警告日志，不要 panic
                    }
                }
            }
        }

        // 2. 准备阶段：重放结束后，为了后续追加写入，以追加模式（Create + Append）再次打开文件
        let append_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_path)
            .await?;

        Ok(Self {
            map: Arc::new(RwLock::new(map)),
            wal_file: Arc::new(Mutex::new(append_file)),
            file_path,
        })
    }

    pub async fn compact(&self) -> Result<()> {
        let tmp_path = format!("{}.tmp", self.file_path);

        let mut tmp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;

        {
            // 获取内存 RwLock 的读锁
            // 思考：为什么拿读锁？因为我们要遍历导出数据。
            // 代价：在此期间，所有的写操作（set/rm）会被阻塞，但读操作（get）可以并发进行。这叫 Stop-The-World (STW)。
            let locked_map = self.map.read().await;

            // 遍历locked_map内存中的 HashMap，将每个键值对写入临时文件tmp_file
            for (key, value) in locked_map.iter() {
                let log_line = format!("S\t{}\t{}\n", key, value);
                tmp_file.write_all(log_line.as_bytes()).await?;
            }

            // 强制刷盘，确保数据写入磁盘
            tmp_file.flush().await?;
        }


        // 原子替换文件
        {
            // 获取文件的独占锁，确保此刻没有 set 正在写入旧文件
            let mut wal = self.wal_file.lock().await;
            
            // 执行操作系统的原子重命名
            tokio::fs::rename(&tmp_path, &self.file_path).await?;

            // 重新以追加模式打开新的 data.log
            let new_file = OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(&self.file_path)
                .await?;
            
            // 将锁里的旧文件句柄替换为新文件句柄！
            *wal = new_file;
        }

        Ok(())
    }
}
*/

impl DiskStorage {
    pub async fn new(path: impl Into<String>) -> Result<Self> {
        let file_path = path.into();
        let mut index = HashMap::new();
        
        // 这是一个追踪文件读取位置的游标
        let mut current_file_offset = 0u64;

        // 1. 重放阶段：以只读模式打开文件并构建索引
        if let Ok(file) = tokio::fs::File::open(&file_path).await {
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            while let Some(line) = lines.next_line().await? {
                // 在 Rust 中，String::len() 返回的是 UTF-8 的【字节数】，而不是字符数。
                // 记得加上 '\n' 的 1 个字节
                let line_byte_len = line.len() as u64 + 1; 

                let parts: Vec<&str> = line.split('\t').collect();
                match parts.as_slice() {
                    ["S", key, value] => {
                        // TODO: 计算当前这个 Value 的精确物理起始位置。
                        // 提示：S\t{key}\t{value}\n
                        // 前置有 "S\t" (2字节)，以及 key的字节数，再加一个 '\t' (1字节)
                        let value_offset = current_file_offset + 2 + key.len() as u64 + 1;
                        let value_len = value.len() as u32;

                        index.insert(key.to_string(), CommandPos {
                            offset: value_offset,
                            length: value_len,
                        });
                    },
                    ["D", key] => {
                        index.remove(*key);
                    },
                    _ => {}
                }

                // 推进游标，准备处理下一行
                current_file_offset += line_byte_len;
            }
        }

        // 2. 准备阶段：打开两个句柄
        
        // A. 异步追加写句柄 (Tokio File)
        let append_file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&file_path)
            .await?;
            
        // B. 同步并发读句柄 (std::fs::File)
        // 这个纯纯只能用于读，依靠操作系统的 read_at 进行多线程无锁并发！
        let sync_reader = std::fs::OpenOptions::new()
            .read(true)
            .open(&file_path)?;

        Ok(Self {
            index: Arc::new(RwLock::new(index)),
            // 将文件末尾的偏移量作为写入游标的初始状态，打包进 Mutex
            wal_writer: Arc::new(Mutex::new((append_file, current_file_offset))),
            wal_reader: Arc::new(sync_reader),
            file_path,
        })
    }
    
    pub async fn compact(&self) -> Result<()> {
        let tmp_path = format!("{}.tmp", self.file_path);

        // 1. 创建全新的临时文件
        let mut tmp_file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .await?;

        let mut new_current_offset = 0u64;

        // ============ 临界区开始 (Stop The World) ============
        {
            // 获取全局写锁，阻塞所有的并发 set/get/remove
            let mut index_guard = self.index.write().await;
            let mut writer_guard = self.wal_writer.lock().await;

            for (key, pos) in index_guard.iter_mut() {
                // 1. 根据旧的 offset，从文件中读取旧的 value
                let mut value_buf = vec![0u8; pos.length as usize];
                
                // 注意：这里没有 .await! 因为 read_at 是同步 API。
                // 虽然在 async 环境下调用少量同步 API 会有极微小的阻塞，但对于紧凑的本地 SSD 顺序压缩，可暂时容忍。
                self.wal_reader.read_at(&mut value_buf, pos.offset)?;

                // 2. 将字节数组无损转回 String
                let value_str = String::from_utf8(value_buf)
                    .map_err(|_| crate::error::KvError::ParseError("Invalid UTF-8 in DB".to_string()))?;

                // 3. 按照物理协议格式化为日志行
                let log_line = format!("S\t{}\t{}\n", key, value_str);
                let log_bytes = log_line.as_bytes();

                // 4. 写入临时文件
                use tokio::io::AsyncWriteExt;
                tmp_file.write_all(log_bytes).await?;

                // 5. TODO: 计算这个 Value 在【临时文件】中的全新起始偏移量
                // 提示：与 new() 函数里的逻辑一致，利用 new_current_offset
                let new_value_offset = new_current_offset + 2 + key.len() as u64 + 1;

                // 6. 就地更新内存中 HashMap 的索引游标！
                // 巧妙设计：index_guard.iter_mut()获取的是内存哈希中CommandPos可变借用
                // 我们在把数据重新写入临时文件的同时，顺便把内存里记录的旧偏移量直接修改成了新文件里的偏移量。
                // 这样压缩完成后，内存索引无缝对接，完全不需要重新构建。
                pos.offset = new_value_offset;

                // 7. TODO: 更新 new_current_offset，让游标前进当前这整行日志的字节数
                new_current_offset += log_bytes.len() as u64;
            }

            tmp_file.flush().await?;

            // 8. 操作系统原子替换文件
            tokio::fs::rename(&tmp_path, &self.file_path).await?;

            // 9. 重新以追加模式打开文件，供后续的 set 操作使用
            let new_append_file = tokio::fs::OpenOptions::new()
                .read(true)
                .append(true)
                .create(true)
                .open(&self.file_path)
                .await?;

            // 10. TODO: 将 writer_guard 解引用，将其底层的 (File, u64) 替换为 (new_append_file, new_current_offset)
            // 提示：利用解引用操作符 *
            *writer_guard = (new_append_file, new_current_offset);

        } // ============ 临界区结束，释放所有锁 ============

        Ok(())
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new()))
        }
    }
}

/*
implement version 1.0

impl StorageEngine for MemoryStorage {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key, value);
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        match self.map.get(&key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }
    /*
    fn get(&self, key: String) -> Result<Option<String>> {
        // get 返回 Option<&String>
        // .cloned() 把它变成了 Option<String>
        // Ok() 把整个东西包成了 Result
        Ok(self.map.get(&key).cloned()) 
    }
    */

    fn remove(&mut self, key: String) -> Result<()> {
        match self.map.remove(&key) {
            Some(_) => Ok(()),
            None => Err(KvError::KeyNotFound("Key not found".to_string())),
        }
    }
}
*/


/*
implement version 2.0

impl StorageEngine for MemoryStorage {
    fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        self.map.insert(key.into(), value.into());
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<String>> {
        Ok(self.map.get(key).cloned()) 
        // get 返回 Option<&String>，.cloned() 把它变成了 Option<String>，Ok() 把整个东西包成了 Result
    }

    fn remove(&mut self, key: impl Into<String>) -> Result<()> {
        let key = key.into();
        match self.map.remove(&key) {
            Some(_) => Ok(()),
            None => Err(KvError::KeyNotFound(key)), // 返回 KeyNotFound 错误
        }
    }
}


impl StorageEngine for DiskStorage {
    fn set(&mut self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        self.map.insert(key.into(), value.into());
        let content = serde_json::to_string(&self.map)?;
        std::fs::write(&self.file_path, content)?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<String>> {
        match self.map.get(key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: impl Into<String>) -> Result<()> {
        let k = key.into();
        match self.map.remove(&k) {
            Some(_) => {
                let content = serde_json::to_string(&self.map)?;
                //正因为remove，内存中的map已经被修改了，所以需要重新写入硬盘
                std::fs::write(&self.file_path, content)?;
                Ok(())
            },
            None => Err(KvError::KeyNotFound(k)),

        }
    }
}
*/

impl StorageEngine for MemoryStorage {
    async fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        // let mut locked_map = self.map.write().unwrap(); // 获取写锁
        let mut locked_map = self.map.write().await;
        locked_map.insert(key.into(), value.into());
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        // let locked_map = self.map.read().unwrap(); // 获取读锁
        let locked_map = self.map.read().await;
        Ok(locked_map.get(key).cloned())
    }

    async fn remove(&self, key: impl Into<String>) -> Result<()> {
        // let mut locked_map = self.map.write().unwrap();
        let mut locked_map = self.map.write().await;
        let k = key.into();
        match locked_map.remove(&k) {
            Some(_) => Ok(()),
            None => Err(KvError::KeyNotFound(k)),
        }
    }
}
// unwrap()
// 一旦线程 A 在持锁期间崩溃，Rust 会立刻将这个锁标记为**“已投毒（Poisoned）”**。
// 当线程 B 来获取锁时，Rust 不会给他锁，而是返回一个 Err(PoisonError)，
// 警告他：“上一个拿锁的人死在里面了，数据可能脏了，你千万别碰！”
// 那我们为什么要 unwrap() 呢？
// 因为在绝大多数后端工程中，如果一个共享的数据结构被写崩溃了，说明代码有致命 Bug。
// 此时最安全的做法就是“让当前线程也跟着一起崩溃（Panic）”，也就是 Fail Fast（快速失败）。
// 直接 unwrap() 遇到 Err 就会触发 Panic，
// 这正是我们期望的——与其继续操作脏数据，不如直接挂掉报错，触发服务器重启。
// 这就是为什么在锁的操作上，标准做法全是 unwrap()！



/*
impl for DiskStorage version 3.0

impl StorageEngine for DiskStorage {

    /*
    async fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        // let mut locked_map = self.map.write().unwrap(); // 获取写锁
        let mut locked_map = self.map.write().await;
        locked_map.insert(key.into(), value.into());

        // 将内存中的 HashMap 序列化为 JSON 字符串，并写入文件
        // &*locked_map 解引用 Arc<RwLock<HashMap<...>>>，得到 HashMap 的引用
        let content = serde_json::to_string(&*locked_map)?;
        // std::fs::write(&self.file_path, content)?;

        // 但是上面有一把全局写锁还没释放，tokio::fs::write 是执行很慢的网络/磁盘io
        // 如果我们在持锁期间去做慢操作，其他线程像是get读取数据就会被阻塞，性能会很差。
        // 所以我们要把锁的作用域缩小，先把内存数据序列化成字符串，然后释放锁，再去写磁盘。
        // 这样其他线程就可以在我们写磁盘的时候去读取数据了，性能就会好很多。
        // 但是我们要注意，写磁盘的过程中，其他线程可能又去 set 了新的数据，导致我们写磁盘的时候把新数据覆盖掉了。
        // 这是一个典型的“读-改-写”竞态条件（read-modify-write race condition），
        // 我们要在文档里明确说明这个问题，或者在设计上避免这个问题，比如使用事务或者版本号等机制。
        // 这里我们就简单地在文档里说明这个问题，暂时不做复杂的处理。
        //
        // 另外，tokio::fs::write 是异步的，如果我们在持锁期间去 await 一个异步操作，可能会导致死锁，
        // 因为 tokio 的调度器可能会在我们持锁期间切换到其他任务，而其他任务可能也在等待这个锁，导致死锁。
        // 为了避免这个问题，我们要在持锁期间不要去 await 异步操作，所以我们要把锁的作用域缩小，
        // 先把内存数据序列化成字符串，然后释放锁，再去写磁盘。
        // 具体修改见下面的代码。
        tokio::fs::write(&self.file_path, content).await?;
        Ok(())
    }
    */


    async fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        let content = { // 开辟一个小作用域
            let mut locked_map = self.map.write().await;// 获取写锁

            // 将内存中的 HashMap 序列化为 JSON 字符串，并写入文件
            // &*locked_map 解引用 Arc<RwLock<HashMap<...>>>，得到 HashMap 的引用
            locked_map.insert(key.into(), value.into());
            serde_json::to_string(&*locked_map)?
        }; // 离开作用域，locked_map 自动销毁，写锁瞬间释放！
        
        // 此时别的客户端已经可以并发去 get 数据了！
        // 我们自己在这个任务里慢慢去写磁盘
        tokio::fs::write(&self.file_path, content).await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        // let locked_map = self.map.read().unwrap(); // 获取读锁
        let locked_map = self.map.read().await;
        match locked_map.get(key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }

    async fn remove(&self, key: impl Into<String>) -> Result<()> {
        let key = key.into();
        // let mut locked_map = self.map.write().unwrap();
        let mut locked_map = self.map.write().await;
        match locked_map.remove(&key) {
            Some(_) => {
                let content = serde_json::to_string(&*locked_map)?;
                // std::fs::write(&self.file_path, content)?;
                tokio::fs::write(&self.file_path, content).await?;
                Ok(())
            }
            None => Err(KvError::KeyNotFound(key)),
        }
    }
}
*/


/*
impl for DiskStorage version 4.0

impl StorageEngine for DiskStorage {
    async fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        let k = key.into();
        let v = value.into();

        // 1. 格式化日志行。格式标准："S\t{key}\t{value}\n"
        let log_line = format!("S\t{}\t{}\n", k, v);

        // 2. 写入 WAL 文件 (先写磁盘)
        {
            // todo: 获取 wal_file 的 Mutex 锁 (需要 await)
            let mut file = self.wal_file.lock().await;
            
            // todo: 将 log_line 转换为字节切片，并使用 write_all 写入
            file.write_all(log_line.as_bytes()).await?;
            file.flush().await?;// 确保数据写入磁盘 AsyncWriteExt trait 提供 flush 方法
        } // 离开作用域，自动释放文件互斥锁

        // 3. 更新内存索引 (后更新内存)
        {
            // todo: 获取 map 的 RwLock 写锁，执行 insert
            let mut locked_map = self.map.write().await;
            locked_map.insert(k, v);
        }
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        let locked_map = self.map.read().await;
        match locked_map.get(key) {
            Some(val) => Ok(Some(val.clone())),
            None => Ok(None),
        }
    }

    async fn remove(&self, key: impl Into<String>) -> Result<()> {
        let k = key.into();
        let log_line = format!("D\t{}\n", k);

        // 错误写法
        // {
        //     let mut locked_map = self.map.write().await;
        //     match locked_map.remove(&k) {
        //         Some(_) => {
        //             let mut file = self.wal_file.lock().await;
        //             file.write_all(log_line.as_bytes()).await?;
        //             Ok(())
        //         },
        //         None => Err(KvError::KeyNotFound(k)),
        //     }
        // }

        {
            let mut file = self.wal_file.lock().await;
            file.write_all(log_line.as_bytes()).await?;
            file.flush().await?;// 确保数据写入磁盘 AsyncWriteExt trait 提供 flush 方法
        }

        {
            let mut locked_map = self.map.write().await;
            match locked_map.remove(&k) {
                Some(_) => Ok(()),
                None => Err(KvError::KeyNotFound(k)),
            }
        }

    }
}
*/


impl StorageEngine for DiskStorage {
    async fn set(&self, key: impl Into<String>, value: impl Into<String>) -> Result<()> {
        let k = key.into();
        let v = value.into();
        let log_line = format!("S\t{}\t{}\n", k, v);
        let log_bytes = log_line.as_bytes();
        let write_len = log_bytes.len() as u32;

        let offset = {
            // 1. 获取追加写锁
            let mut writer_guard = self.wal_writer.lock().await;
            let (file, current_offset) = &mut *writer_guard;

            // file: &mut File
            // current_offset: &mut u64
            // 所以后面要使用 *current_offset 解引用指向实际的偏移量并复制给 start_offset

            // 2. 拿到当前的游标，作为本次数据的写入起点
            let start_offset = *current_offset;

            // 3. 执行物理写入并刷盘
            use tokio::io::AsyncWriteExt;
            file.write_all(log_bytes).await?;
            file.flush().await?;

            // 4. 更新游标位置（前移）
            *current_offset += write_len as u64;

            start_offset
        }; // 锁释放

        // 5. 更新内存索引
        // 注意：你要存入的长度是【Value的长度】还是【整行日志的长度】？
        // 建议索引只记录 Value 在文件中的绝对偏移量和长度，这样 get 的时候直接切出 Value 即可，不需要再做字符串 split！
        // TODO: 计算仅仅是 v (Value) 在日志行中的物理偏移量和长度。
        // 提示：S\t{key}\t{value}\n，Value 的绝对起始偏移量 = start_offset + 2 + key.len() + 1
        let value_offset = offset + 2 + k.len() as u64 + 1;
        let value_len = v.len() as u32;

        let mut index = self.index.write().await;
        index.insert(k, CommandPos {
            offset: value_offset,
            length: value_len,
        });

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<String>> {
        // 1. 查内存索引
        let index = self.index.read().await;
        let pos = match index.get(key) {
            Some(p) => p.clone(),
            None => return Ok(None),
        };
        // 释放读锁，不要占着茅坑拉屎，接下来的磁盘 I/O 非常慢
        drop(index); 

        // 2. 准备系统调用需要的数据
        // 将 Arc 克隆一份，准备送入另一个线程
        let reader = Arc::clone(&self.wal_reader);
        
        // 3. 将阻塞的同步磁盘读取放入 spawn_blocking
        let result = tokio::task::spawn_blocking(move || {
            // 创建一个精确大小的缓冲区
            let mut buf = vec![0u8; pos.length as usize];
            
            // TODO: 调用 std::os::unix::fs::FileExt::read_at
            // 将文件在 pos.offset 处的数据读取到 buf 中
            reader.read_at(&mut buf, pos.offset)?;
            
            // 将纯净的字节反序列化为 String
            let value_str = String::from_utf8(buf)
                .map_err(|_| crate::error::KvError::ParseError("Invalid utf8 in db".to_string()))?;
            // .map_err(|_|) 当 Result 处于 Err 状态时，把原有的底层错误吞掉（用 _ 忽略），
            // 并映射（Map）转换成我们自定义的错误类型”。
                
            // 注意这里需要明确指明返回值的错误类型，匹配最外层的闭包要求
            Ok::<String, crate::error::KvError>(value_str)
            // Ok::<成功时的类型, 错误时的类型>(返回值)
            // 这里尽量不能写 Ok(value_str) 
            // 在 Rust 的闭包内部，由于使用了 ? 运算符，编译器需要推导这个闭包最终返回的 Result<T, E> 的具体类型。
            // 如果只写 Ok(value_str)，编译器有时无法准确推断出错误类型 E 到底是什么（是 std::io::Error 还是 KvError）。

        }).await.expect("Spawn blocking failed"); // 这里 expect 捕获的是线程崩溃的恐慌

        // 返回解包后的结果
        match result {
            Ok(val) => Ok(Some(val)),
            Err(e) => Err(e),
        }
    }
    
    // TODO: remove 方法自己推导。提示：WAL 里追加一条 "D\tkey\n"，然后把 key 从 HashMap 里 remove 掉。
    async fn remove(&self, key: impl Into<String>) -> Result<()> {
        let k = key.into();
        let log_line = format!("D\t{}\n", k);
        let log_bytes = log_line.as_bytes();
        let write_len = log_bytes.len() as u32;


        // 这里和上面的set不同 不需要知道start_offset 不需要查磁盘 因为后面不需要算内存value索引
        // 只需要在 WAL 里追加一条删除记录即可 并从内存索引中移除该 key
        {
            // let mut writer = self.wal_writer.lock().await;
            // let start_offset = writer.current_offset();
            // writer.write_all(log_bytes).await?;
            // let write_len = log_bytes.len() as u32;
            // *writer.current_offset_mut() += write_len as u64;
            // start_offset

            let mut writer_guard = self.wal_writer.lock().await;
            let (file, current_offset) = &mut *writer_guard;

            let start_offset = *current_offset;

            file.write_all(log_bytes).await?;
            file.flush().await?;

            *current_offset += write_len as u64;
        }

        let mut index = self.index.write().await;
        index.remove(&k);

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*; // 将同文件上方的 DiskStorage, StorageEngine 统统引入
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_concurrent_set_get() {
        // 1. 获取物理隔离的沙盒目录
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test_concurrent.log");
        let db_path_str = db_path.to_str().unwrap().to_string();

        // 2. 初始化引擎
        let engine = Arc::new(DiskStorage::new(db_path_str).await.unwrap());

        // 3. M:N 调度并发写测试
        let mut handles = vec![];
        for i in 0..100 {
            let engine_clone = Arc::clone(&engine);
            // 将每一个写操作扔进独立的 tokio 绿色线程（Task）
            let handle = tokio::spawn(async move {
                let key = format!("key_{}", i);
                let val = format!("val_{}", i);
                // 引擎的 set 内部包含了追加写锁和 HashMap 锁，这里测试它们会不会死锁
                engine_clone.set(key, val).await.unwrap();
            });
            handles.push(handle);
        }

        // 4. Join 点：阻塞等待这 100 个并发任务全部执行完毕
        // 主测试线程会在这里卡住（等待），直到这 100 个并发写任务全部执行完毕。
        // 如果中途哪个任务发生 Panic（恐慌崩溃），unwrap() 会立刻捕获并让整个测试报错失败。
        for h in handles {
            h.await.unwrap();
        }

        // 5. 验证读的正确性
        // TODO: 写一个 0..100 的 for 循环。
        // 在循环内：
        //   A. 计算出期望的 key (如 "key_0") 和期望的 val (如 "val_0")
        //   B. 调用 engine.get 去查询，用 unwrap() 把 Result 和 Option 都解开。
        //      第一个 unwrap()：解开 tokio::task::spawn_blocking 返回的 Result（捕获线程恐慌）。
        //      第二个 unwrap()：解开 StorageEngine::get() 返回的 Result<Option<String>> 中的 Option（确保 Key 一定存在，若为 None 则直接报错）。
        //   C. 使用标准库的 assert_eq!(actual, expected) 断言它们绝对相等。
        for i in 0..100 {
            let key = format!("key_{}", i);
            let expected_val = format!("val_{}", i);
            let actual_val = engine.get(&key).await.unwrap().unwrap();
            assert_eq!(actual_val, expected_val);
        }
    }
}