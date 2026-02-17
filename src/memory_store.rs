use async_trait::async_trait;
use bytes::Bytes;
use gbe_state_store::{Record, ScanFilter, ScanOp, StateStore, StateStoreError};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// In-memory StateStore for testing and `--backend memory` mode.
pub struct MemoryStateStore {
    data: Mutex<HashMap<String, Record>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl StateStore for MemoryStateStore {
    async fn get(&self, key: &str) -> Result<Option<Record>, StateStoreError> {
        let data = self.data.lock().unwrap();
        Ok(data.get(key).cloned())
    }

    async fn put(
        &self,
        key: &str,
        record: Record,
        _ttl: Option<Duration>,
    ) -> Result<(), StateStoreError> {
        let mut data = self.data.lock().unwrap();
        data.insert(key.to_string(), record);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StateStoreError> {
        let mut data = self.data.lock().unwrap();
        data.remove(key);
        Ok(())
    }

    async fn get_field(&self, key: &str, field: &str) -> Result<Option<Bytes>, StateStoreError> {
        let data = self.data.lock().unwrap();
        Ok(data
            .get(key)
            .and_then(|r| r.fields.get(field).cloned()))
    }

    async fn set_field(
        &self,
        key: &str,
        field: &str,
        value: Bytes,
    ) -> Result<(), StateStoreError> {
        let mut data = self.data.lock().unwrap();
        let record = data.entry(key.to_string()).or_default();
        record.fields.insert(field.to_string(), value);
        Ok(())
    }

    async fn set_fields(
        &self,
        key: &str,
        fields: HashMap<String, Bytes>,
    ) -> Result<(), StateStoreError> {
        let mut data = self.data.lock().unwrap();
        let record = data.entry(key.to_string()).or_default();
        for (k, v) in fields {
            record.fields.insert(k, v);
        }
        Ok(())
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        field: &str,
        expected: Bytes,
        new: Bytes,
    ) -> Result<bool, StateStoreError> {
        let mut data = self.data.lock().unwrap();
        let record = data.entry(key.to_string()).or_default();
        match record.fields.get(field) {
            Some(current) if *current == expected => {
                record.fields.insert(field.to_string(), new);
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    async fn scan(
        &self,
        prefix: &str,
        filter: Option<ScanFilter>,
    ) -> Result<Vec<(String, Record)>, StateStoreError> {
        let data = self.data.lock().unwrap();
        let mut results: Vec<(String, Record)> = data
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        if let Some(f) = filter {
            results.retain(|(_, record)| {
                if let Some(val) = record.fields.get(&f.field) {
                    match f.op {
                        ScanOp::Eq => *val == f.value,
                        ScanOp::Lt => *val < f.value,
                        ScanOp::Gt => *val > f.value,
                    }
                } else {
                    false
                }
            });
            if let Some(max) = f.max_results {
                results.truncate(max as usize);
            }
        }

        Ok(results)
    }

    async fn ping(&self) -> Result<bool, StateStoreError> {
        Ok(true)
    }

    async fn close(&self) -> Result<(), StateStoreError> {
        Ok(())
    }
}
