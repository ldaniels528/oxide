#![warn(dead_code)]
////////////////////////////////////////////////////////////////////
// Connections enumeration
////////////////////////////////////////////////////////////////////

use crate::byte_code_compiler::ByteCodeCompiler;
use crate::connections::Connections::{BLOBStoreHandle, WebServerHandle, WebSocketHandle};
use crate::typed_values::TypedValue;
use crate::typed_values::TypedValue::Connection;
use crate::utils::u128_to_uuid;
use serde::{Deserialize, Serialize};

/// Represents a Connections enumeration
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Connections {
    BLOBStoreHandle(u128),
    WebServerHandle(u16),
    WebSocketHandle(u128),
}

impl Connections {

    pub fn close(&self) -> std::io::Result<bool> {
        match self {
            BLOBStoreHandle(uuid) => blob_stores::close(*uuid).map(|_| true),
            WebSocketHandle(uuid) => websockets::close_blocking(*uuid).map(|_| true),
            WebServerHandle(port) => webservers::stop_server_blocking(*port).map(|_| true),
        }
    }

    pub async fn close_async(&self) -> std::io::Result<bool> {
        match self {
            BLOBStoreHandle(uuid) => blob_stores::close(*uuid).map(|_| true),
            WebServerHandle(port) => webservers::stop_server(*port).await.map(|_| true),
            WebSocketHandle(uuid) => websockets::close(*uuid).await.map(|_| true),
        }
    }

    pub fn encode(&self) -> std::io::Result<Vec<u8>> {
        match self {
            BLOBStoreHandle(id) => Ok(id.to_be_bytes().to_vec()),
            WebServerHandle(port) => Ok(port.to_be_bytes().to_vec()),
            WebSocketHandle(id) => Ok(id.to_be_bytes().to_vec()),
        }
    }

    pub fn get_type(&self) -> ConnectionTypes {
        match self {
            BLOBStoreHandle(..) => ConnectionTypes::BLOBStoreHandleType,
            WebServerHandle(..) => ConnectionTypes::WebServerHandleType,
            WebSocketHandle(..) => ConnectionTypes::WebSocketHandleType,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            WebServerHandle(port) => serde_json::json!(port),
            _ => serde_json::json!(self.unwrap_value())
        }
    }

    pub fn unwrap_value(&self) -> String {
        match self {
            BLOBStoreHandle(id) => u128_to_uuid(*id),
            WebServerHandle(port) => format!("{}", *port),
            WebSocketHandle(id) => u128_to_uuid(*id),
        }
    }

}

/// Represents a Connection Type
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ConnectionTypes {
    BLOBStoreHandleType,
    WebServerHandleType,
    WebSocketHandleType,
}

impl ConnectionTypes {

    pub fn compute_fixed_size(&self) -> usize {
        match self {
            ConnectionTypes::BLOBStoreHandleType => 16,
            ConnectionTypes::WebServerHandleType => 2,
            ConnectionTypes::WebSocketHandleType => 16,
        }
    }

    pub fn decode(&self, buffer: &Vec<u8>, offset: usize) -> TypedValue {
        match self {
            ConnectionTypes::BLOBStoreHandleType => ByteCodeCompiler::decode_u8x16(
                buffer, offset, |b| Connection(BLOBStoreHandle(u128::from_be_bytes(b)))),
            ConnectionTypes::WebServerHandleType => ByteCodeCompiler::decode_u8x2(
                buffer, offset, |b| Connection(WebServerHandle(u16::from_be_bytes(b)))),
            ConnectionTypes::WebSocketHandleType => ByteCodeCompiler::decode_u8x16(
                buffer, offset, |b| Connection(WebSocketHandle(u128::from_be_bytes(b)))),
        }
    }

    pub fn to_code(&self) -> String {
        match self {
            ConnectionTypes::BLOBStoreHandleType => "BLOBStore".into(),
            ConnectionTypes::WebServerHandleType => "WebServer".into(),
            ConnectionTypes::WebSocketHandleType => "WebSocket".into(),
        }
    }

}

/// BLOB store resources
pub mod blob_stores {
    use crate::blobs::{BLOBMetadata, BLOBStore};
    use crate::connections::Connections::BLOBStoreHandle;
    use crate::data_types::DataType::{NumberType, UUIDType};
    use crate::dataframe::Dataframe;
    use crate::dataframe::Dataframe::ModelTable;
    use crate::model_row_collection::ModelRowCollection;
    use crate::namespaces::Namespace;
    use crate::number_kind::NumberKind::U64Kind;
    use crate::numbers::Numbers::U64Value;
    use crate::parameter::Parameter;
    use crate::structures::Row;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::{Boolean, Connection, Number, TableValue, UUIDValue, Undefined};
    use crate::utils;
    use crate::utils::generate_uuid;
    use once_cell::sync::Lazy;
    use shared_lib::cnv_error;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use tokio::sync::{Mutex, MutexGuard};

    static BLOB_STORES: Lazy<Arc<RwLock<HashMap<u128, Arc<Mutex<BLOBStore>>>>>> =
        Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

    pub async fn append(store_id: u128, value: TypedValue) -> std::io::Result<TypedValue> {
        with_store(store_id, Undefined, |store| {
            let bmd = store.insert_value(&value)?;
            Ok(UUIDValue(bmd.blob_id))
        }).await
    }

    pub fn append_blocking(store_id: u128, value: &TypedValue) -> std::io::Result<TypedValue> {
        with_store_blocking(store_id, Undefined, |store| {
            let bmd = store.insert_value(value)?;
            Ok(UUIDValue(bmd.blob_id))
        })
    }

    pub fn close(store_id: u128) -> std::io::Result<TypedValue> {
        println!("Closing BLOB store {}", utils::u128_to_uuid(store_id));
        let result = BLOB_STORES
            .write().map_err(|e| cnv_error!(e))?
            .remove(&store_id);
        Ok(Boolean(result.is_some()))
    }

    pub fn create(namespace: &str) -> std::io::Result<TypedValue> {
        let blobstore = BLOBStore::create(&Namespace::parse(namespace)?)?;
        let store_id = generate_uuid();
        BLOB_STORES
            .write().map_err(|e| cnv_error!(e))?
            .insert(store_id, Arc::new(Mutex::new(blobstore)));
        Ok(Connection(BLOBStoreHandle(store_id)))
    }

    pub async fn entries(store_id: u128) -> std::io::Result<TypedValue> {
        with_store(store_id, Undefined, |store| {
            let entries = store.get_entries()?;
            Ok(TableValue(metadata_to_table(&entries)))
        }).await
    }

    pub fn entries_blocking(store_id: u128) -> std::io::Result<TypedValue> {
        with_store_blocking(store_id, Undefined, |store| {
            let entries = store.get_entries()?;
            Ok(TableValue(metadata_to_table(&entries)))
        })
    }

    pub fn get_metadata_parameters() -> Vec<Parameter> {
        vec![
            Parameter::new("blob_id", UUIDType),
            Parameter::new("offset", NumberType(U64Kind)),
            Parameter::new("allocated", NumberType(U64Kind)),
            Parameter::new("used", NumberType(U64Kind)),
        ]
    }

    fn get_store(store_id: u128) -> std::io::Result<Option<Arc<Mutex<BLOBStore>>>> {
        Ok(BLOB_STORES
            .read().map_err(|e| cnv_error!(e))?
            .get(&store_id).cloned())
    }

    pub async fn len(store_id: u128) -> std::io::Result<u64> {
        with_store(store_id, 0, |store| store.len()).await
    }

    pub fn len_blocking(store_id: u128) -> std::io::Result<u64> {
        with_store_blocking(store_id, 0, |store| store.len())
    }

    pub fn load(namespace: &str) -> std::io::Result<TypedValue> {
        let blobstore = BLOBStore::open(&Namespace::parse(namespace)?)?;
        let store_id = generate_uuid();
        BLOB_STORES
            .write().map_err(|e| cnv_error!(e))?
            .insert(store_id, Arc::new(Mutex::new(blobstore)));
        Ok(Connection(BLOBStoreHandle(store_id)))
    }

    pub fn metadata_to_table(entries: &Vec<BLOBMetadata>) -> Dataframe {
        let rows = entries.iter().enumerate().map(|(n, entry)| Row::new(n, vec![
            UUIDValue(entry.blob_id),
            Number(U64Value(entry.offset)),
            Number(U64Value(entry.allocated)),
            Number(U64Value(entry.used)),
        ])).collect::<Vec<_>>();
        ModelTable(ModelRowCollection::from_parameters_and_rows(
            &get_metadata_parameters(),
            &rows,
        ))
    }

    pub async fn read(store_id: u128, blob_id: u128) -> std::io::Result<TypedValue> {
        with_metadata(store_id, blob_id, Undefined, |store, bmd| store.read_value(&bmd)).await
    }

    pub fn read_blocking(store_id: u128, blob_id: u128) -> std::io::Result<TypedValue> {
        with_metadata_blocking(store_id, blob_id, Undefined, |store, bmd| store.read_value(&bmd))
    }

    pub async fn truncate(store_id: u128) -> std::io::Result<TypedValue> {
        with_store(store_id, Boolean(false), |store| store.truncate().map(|_| Boolean(true))).await
    }

    pub fn truncate_blocking(store_id: u128) -> std::io::Result<TypedValue> {
        with_store_blocking(store_id, Boolean(false), |store| store.truncate().map(|_| Boolean(true)))
    }

    pub async fn update(store_id: u128, blob_id: u128, value: &TypedValue) -> std::io::Result<TypedValue> {
        let value = value.clone();
        with_metadata(store_id, blob_id, Undefined, |store, bmd| {
            let new_bmd = store.update_value(&bmd, &value)?;
            Ok(UUIDValue(new_bmd.blob_id))
        }).await
    }

    pub fn update_blocking(store_id: u128, blob_id: u128, value: &TypedValue) -> std::io::Result<TypedValue> {
        with_metadata_blocking(store_id, blob_id, Undefined, |store, bmd| {
            let new_bmd = store.update_value(&bmd, value)?;
            Ok(UUIDValue(new_bmd.blob_id))
        })
    }

    async fn with_metadata<T>(
        store_id: u128,
        blob_id: u128,
        default: T,
        f: impl FnOnce(&mut BLOBStore, BLOBMetadata) -> std::io::Result<T>) -> std::io::Result<T> {
        match get_store(store_id)? {
            Some(store_mutex) => {
                let mut store = store_mutex.lock().await;
                match store.read_metadata_by_uuid(blob_id)? {
                    None => Ok(default),
                    Some(bmd) => f(&mut store, bmd)
                }
            }
            None => Ok(default),
        }
    }

    fn with_metadata_blocking<F, T>(
        store_id: u128,
        blob_id: u128,
        default: T,
        mut f: F
    ) -> std::io::Result<T>
    where
        F: FnMut(MutexGuard<BLOBStore>, BLOBMetadata) -> std::io::Result<T>,
    {
        match get_store(store_id)? {
            Some(store_mutex) => {
                let store = store_mutex.blocking_lock();
                match store.read_metadata_by_uuid(blob_id)? {
                    None => Ok(default),
                    Some(bmd) => f(store, bmd)
                }
            }
            None => Ok(default),
        }
    }

    async fn with_store<T>(store_id: u128, default: T, f: impl FnOnce(&mut BLOBStore) -> std::io::Result<T>) -> std::io::Result<T> {
        match get_store(store_id)? {
            Some(store_mutex) => {
                let mut store = store_mutex.lock().await;
                f(&mut store)
            }
            None => Ok(default),
        }
    }

    fn with_store_blocking<F, T>(store_id: u128, default: T, mut f: F) -> std::io::Result<T>
    where
        F: FnMut(MutexGuard<BLOBStore>) -> std::io::Result<T>,
    {
        match get_store(store_id)? {
            Some(store_mutex) => {
                let store = store_mutex.blocking_lock();
                f(store)
            }
            None => Ok(default),
        }
    }
}

/// One-time execution resources
pub mod one_time {
    use once_cell::sync::Lazy;
    use shared_lib::cnv_error;
    use std::collections::HashSet;
    use std::sync::{Arc, RwLock};

    static ONE_TIME: Lazy<Arc<RwLock<HashSet<u128>>>> =
        Lazy::new(|| Arc::new(RwLock::new(HashSet::new())));

    pub fn is_triggered(uid: &u128) -> bool {
        ONE_TIME.read().unwrap().contains(uid)
    }

    pub fn if_not_triggered(uid: &u128) -> std::io::Result<bool> {
        let is_already_fired = is_triggered(uid);
        if !is_already_fired {
            ONE_TIME
                .write()
                .map_err(|e| cnv_error!(e))?
                .insert(*uid);
        }
        Ok(!is_already_fired)
    }
}

/// Webserver resources
pub mod webservers {
    use crate::server_engine;
    use crate::server_engine::UserAPI;
    use once_cell::sync::Lazy;
    use rand::prelude::ThreadRng;
    use rand::{thread_rng, Rng};
    use shared_lib::cnv_error;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, RwLock};
    use tokio::sync::Mutex;

    static PORTS: Lazy<Arc<RwLock<HashSet<u16>>>> =
        Lazy::new(|| Arc::new(RwLock::new(HashSet::new())));

    static WEB_SERVERS: Lazy<Arc<RwLock<HashMap<u16, Arc<Mutex<tokio::task::JoinHandle<()>>>>>>> =
        Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

    pub fn get_random_port() -> std::io::Result<u16> {
        let port_range = 4000..65535;
        let mut rng: ThreadRng = thread_rng();
        let mut port: u16 = rng.gen_range(port_range.clone());
        while is_locked(port)? || is_running(port)? {
            port = rng.gen_range(port_range.clone());
        }
        lock_port(port)?;
        println!("get_random_port: port: {}", port);
        Ok(port)
    }

    pub fn is_locked(port: u16) -> std::io::Result<bool> {
        Ok(PORTS.read().map_err(|e| cnv_error!(e))?.contains(&port))
    }

    pub fn is_running(port: u16) -> std::io::Result<bool> {
        Ok(WEB_SERVERS.read().map_err(|e| cnv_error!(e))?.contains_key(&port))
    }

    pub fn lock_port(port: u16) -> std::io::Result<bool> {
        Ok(PORTS
            .write()
            .map_err(|e| cnv_error!(e))?
            .insert(port))
    }

    pub fn unlock_port(port: u16) -> std::io::Result<bool> {
        Ok(PORTS
            .write()
            .map_err(|e| cnv_error!(e))?
            .remove(&port))
    }

    // pub async fn list_servers() -> std::io::Result<Vec<u16>> {
    //     Ok(WEB_SERVERS
    //         .read()
    //         .map_err(|e| cnv_error!(e))?
    //         .keys().cloned().collect())
    // }

    pub async fn start_server(port: u16) -> std::io::Result<()> {
        start_server_with_api(port, vec![]).await
    }

    pub async fn start_server_on_random_port() -> std::io::Result<u16> {
        let port: u16 = get_random_port()?;
        let _ = start_server_with_api(port, vec![]).await?;
        Ok(port)
    }

    pub async fn start_server_with_api(port: u16, apis: Vec<UserAPI>) -> std::io::Result<()> {
        println!("webservers: Starting server on port {}...", port);
        let handle = server_engine::start_http_server_async(port, apis).await;
        WEB_SERVERS
            .write()
            .map_err(|e| cnv_error!(e))?
            .insert(port, Arc::new(Mutex::new(handle)));
        Ok(())
    }

    pub async fn stop_server(port: u16) -> std::io::Result<bool> {
        println!("webservers: Stopping server on port {}...", port);
        unlock_port(port)?;
        let handle_maybe = WEB_SERVERS
            .write()
            .map_err(|e| cnv_error!(e))?
            .remove(&port);
        Ok(match handle_maybe {
            None => false,
            Some(handle) => {
                handle.lock().await.abort();
                true
            },
        })
    }

    pub fn stop_server_blocking(port: u16) -> std::io::Result<bool> {
        println!("webservers: Stopping server on port {}...", port);
        let handle_maybe = WEB_SERVERS
            .write()
            .map_err(|e| cnv_error!(e))?
            .remove(&port);
        Ok(match handle_maybe {
            None => false,
            Some(handle) => {
                handle.blocking_lock().abort();
                true
            },
        })
    }
}

/// Websocket resources
pub mod websockets {
    use crate::connections::Connections::WebSocketHandle;
    use crate::typed_values::TypedValue;
    use crate::typed_values::TypedValue::*;
    use crate::utils::generate_uuid;
    use crate::web_engine::WebSocketClient;
    use once_cell::sync::Lazy;
    use shared_lib::cnv_error;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use tokio::sync::Mutex;

    static WEBSOCKET_REGISTRY: Lazy<Arc<RwLock<HashMap<u128, Arc<Mutex<WebSocketClient>>>>>> =
        Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

    pub async fn close(id: u128) -> std::io::Result<TypedValue> {
        match get_client(id)? {
            Some(client_arc) => {
                let mut client = client_arc.lock().await;
                client.close().await?;
                client.read_next().await
            }
            None => Ok(Undefined),
        }
    }

    pub fn close_blocking(_id: u128) -> std::io::Result<TypedValue> {
        Ok(Undefined)
    }

    pub async fn connect_ws(host: &str, port: u16, path: &str) -> std::io::Result<TypedValue> {
        println!("connect_ws: Connecting to {}:{}{}...", host, port, path);
        let client = WebSocketClient::connect(host, port, path).await?;
        println!("connect_ws: Connected to {}:{}{}...", host, port, path);
        let id = generate_uuid();
        WEBSOCKET_REGISTRY
            .write().map_err(|e| cnv_error!(e))?
            .insert(id, Arc::new(Mutex::new(client)));
        Ok(Connection(WebSocketHandle(id)))
    }

    pub async fn send_binary_command(id: u128, msg: Vec<u8>) -> std::io::Result<TypedValue> {
        match get_client(id)? {
            Some(client_arc) => {
                let mut client = client_arc.lock().await;
                client.send_binary_message(msg).await?;
                client.read_next().await
            }
            None => Ok(Undefined),
        }
    }

    pub async fn send_text_command(id: u128, msg: &str) -> std::io::Result<TypedValue> {
        match get_client(id)? {
            Some(client_arc) => {
                let mut client = client_arc.lock().await;
                client.send_text_message(msg).await?;
                client.read_next().await
            }
            None => Ok(Undefined),
        }
    }

    fn get_client(id: u128) -> std::io::Result<Option<Arc<Mutex<WebSocketClient>>>> {
        Ok(WEBSOCKET_REGISTRY
            .read().map_err(|e| cnv_error!(e))?
            .get(&id).cloned())
    }
}

/// Unit tests
#[cfg(test)]
mod tests {

    /// Unit tests
    #[cfg(test)]
    mod connection_tests {
        use crate::connections::Connections::*;
        use serde_json::json;

        #[test]
        fn test_encode_value_blobstore() {
            assert_eq!(
                BLOBStoreHandle(0x0feb833c_6a93_4a0c_9ed3_cb05d1629366).encode().unwrap(),
                0x0feb833c_6a93_4a0c_9ed3_cb05d1629366u128.to_be_bytes().to_vec());
        }

        #[test]
        fn test_encode_value_webserver() {
            assert_eq!(
                WebServerHandle(567).encode().unwrap(),
                567u16.to_be_bytes().to_vec());
        }

        #[test]
        fn test_encode_value_websocket() {
            assert_eq!(
                WebSocketHandle(0x0feb833c_6a93_4a0c_9ed3_cb05d1629366).encode().unwrap(),
                0x0feb833c_6a93_4a0c_9ed3_cb05d1629366u128.to_be_bytes().to_vec());
        }

        #[test]
        fn test_to_json_blobstore() {
            assert_eq!(
                BLOBStoreHandle(0x0feb833c_6a93_4a0c_9ed3_cb05d1629366).to_json(),
                json!("0feb833c-6a93-4a0c-9ed3-cb05d1629366"));
        }

        #[test]
        fn test_to_json_webserver() {
            assert_eq!(WebServerHandle(567).to_json(), json!(567));
        }

        #[test]
        fn test_to_json_websocket() {
            assert_eq!(
                WebSocketHandle(0x0feb833c_6a93_4a0c_9ed3_cb05d1629366).to_json(),
                json!("0feb833c-6a93-4a0c-9ed3-cb05d1629366"));
        }

        #[test]
        fn test_unwrap_value_blobstore() {
            assert_eq!(
                BLOBStoreHandle(0x0feb833c_6a93_4a0c_9ed3_cb05d1629366).unwrap_value(),
                "0feb833c-6a93-4a0c-9ed3-cb05d1629366");
        }

        #[test]
        fn test_unwrap_value_webserver() {
            assert_eq!(WebServerHandle(567).unwrap_value(), "567");
        }

        #[test]
        fn test_unwrap_value_websocket() {
            assert_eq!(
                WebSocketHandle(0x0feb833c_6a93_4a0c_9ed3_cb05d1629366).unwrap_value(),
                "0feb833c-6a93-4a0c-9ed3-cb05d1629366");
        }

    }

    mod connection_type_tests {
        use crate::connections::ConnectionTypes::*;
        use crate::connections::Connections;
        use crate::connections::Connections::*;
        use crate::typed_values::TypedValue::Connection;

        #[test]
        fn test_compute_size() {
            assert_eq!(BLOBStoreHandleType.compute_fixed_size(), 16);
            assert_eq!(WebServerHandleType.compute_fixed_size(), 2);
            assert_eq!(WebSocketHandleType.compute_fixed_size(), 16);
        }

        #[test]
        fn test_decode_blobstore() {
            verify_decode(BLOBStoreHandle(0x0feb833c_6a93_4a0c_9ed3_cb05d1629366));
        }

        #[test]
        fn test_decode_webserver() {
            verify_decode(WebServerHandle(8888));
        }

        #[test]
        fn test_decode_websocket() {
            verify_decode(WebSocketHandle(0x0feb833c_6a93_4a0c_9ed3_cb05d1629366));
        }

        #[test]
        fn test_to_code() {
            assert_eq!(BLOBStoreHandleType.to_code(), "BLOBStore");
            assert_eq!(WebServerHandleType.to_code(), "WebServer");
            assert_eq!(WebSocketHandleType.to_code(), "WebSocket");
        }

        fn verify_decode(expected: Connections) {
            let bytes = expected.encode().unwrap();
            let my_type = expected.get_type();
            let actual = my_type.decode(&bytes, 0);
            assert_eq!(Connection(expected), actual);
            assert_eq!(my_type.compute_fixed_size(), bytes.len());
        }
    }

}