use crate::error::RunnerError;
use async_trait::async_trait;
use bytes::Bytes;
use gbe_jobs_domain::payloads::{
    JobCompleted, JobCreated, JobFailed, TaskCompleted, TaskFailed, TaskQueued,
};
use gbe_jobs_domain::subjects;
use gbe_jobs_domain::{
    keys, JobDefinition, JobId, JobState, OrgId, TaskDefinition, TaskId, TaskState, TaskType,
};
use gbe_nexus::Transport;
use gbe_state_store::{Record, StateStore};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Domain-level state operations for the runner.
#[async_trait]
pub trait StateManager: Send + Sync {
    async fn create_job(
        &self,
        job_id: &JobId,
        org_id: &OrgId,
        def: &JobDefinition,
        task_ids: &[(String, TaskId)],
    ) -> Result<(), RunnerError>;

    async fn dispatch_task(
        &self,
        task_id: &TaskId,
        job_id: &JobId,
        org_id: &OrgId,
        task_def: &TaskDefinition,
    ) -> Result<(), RunnerError>;

    async fn complete_task(
        &self,
        task_id: &TaskId,
        job_id: &JobId,
        task_type: &TaskType,
    ) -> Result<(), RunnerError>;

    async fn fail_task(
        &self,
        task_id: &TaskId,
        job_id: &JobId,
        task_type: &TaskType,
        error: &str,
    ) -> Result<(), RunnerError>;

    async fn complete_job(&self, job_id: &JobId, org_id: &OrgId, job_type: &str)
        -> Result<(), RunnerError>;

    async fn fail_job(
        &self,
        job_id: &JobId,
        org_id: &OrgId,
        job_type: &str,
        failed_task_id: &TaskId,
        error: &str,
    ) -> Result<(), RunnerError>;
}

/// Production StateManager backed by StateStore + Transport.
pub struct NexusStateManager {
    store: Arc<dyn StateStore>,
    transport: Arc<dyn Transport>,
}

impl NexusStateManager {
    pub fn new(store: Arc<dyn StateStore>, transport: Arc<dyn Transport>) -> Self {
        Self { store, transport }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[async_trait]
impl StateManager for NexusStateManager {
    async fn create_job(
        &self,
        job_id: &JobId,
        org_id: &OrgId,
        def: &JobDefinition,
        task_ids: &[(String, TaskId)],
    ) -> Result<(), RunnerError> {
        let now = now_ms();

        // Write job record
        let job_key = keys::job_key(&def.job_type, job_id.as_str());
        let mut fields = HashMap::new();
        fields.insert(keys::fields::job::STATE.into(), Bytes::from(JobState::Running.as_str()));
        fields.insert(keys::fields::job::JOB_TYPE.into(), Bytes::from(def.job_type.clone()));
        fields.insert(keys::fields::job::JOB_ID.into(), Bytes::from(job_id.as_str().to_string()));
        fields.insert(keys::fields::job::ORG_ID.into(), Bytes::from(org_id.as_str().to_string()));
        fields.insert(
            keys::fields::job::TASK_COUNT.into(),
            Bytes::from(def.tasks.len().to_string()),
        );
        fields.insert(keys::fields::job::COMPLETED_COUNT.into(), Bytes::from("0"));
        fields.insert(keys::fields::job::FAILED_COUNT.into(), Bytes::from("0"));
        fields.insert(keys::fields::job::CREATED_AT.into(), Bytes::from(now.to_string()));
        self.store
            .put(&job_key, Record { fields, ttl: None }, None)
            .await?;

        // Write task records + index entries
        for (task_name, task_id) in task_ids {
            let task_def = def.tasks.iter().find(|t| &t.name == task_name).unwrap();
            let task_key = keys::task_key(task_def.task_type.as_str(), task_id.as_str());
            let initial_state = if task_def.depends_on.is_empty() {
                TaskState::Pending
            } else {
                TaskState::Blocked
            };

            let mut tfields = HashMap::new();
            tfields.insert(keys::fields::task::STATE.into(), Bytes::from(initial_state.as_str()));
            tfields.insert(
                keys::fields::task::TASK_TYPE.into(),
                Bytes::from(task_def.task_type.as_str().to_string()),
            );
            tfields.insert(
                keys::fields::task::TASK_ID.into(),
                Bytes::from(task_id.as_str().to_string()),
            );
            tfields.insert(
                keys::fields::task::JOB_ID.into(),
                Bytes::from(job_id.as_str().to_string()),
            );
            tfields.insert(
                keys::fields::task::ORG_ID.into(),
                Bytes::from(org_id.as_str().to_string()),
            );
            tfields.insert(keys::fields::task::TASK_NAME.into(), Bytes::from(task_name.clone()));
            tfields.insert(keys::fields::task::CREATED_AT.into(), Bytes::from(now.to_string()));

            self.store
                .put(&task_key, Record { fields: tfields, ttl: None }, None)
                .await?;

            // Index: job -> task by name
            let idx_key = keys::job_task_index_key(job_id.as_str(), task_name);
            let mut idx_fields = HashMap::new();
            idx_fields.insert(
                "task_id".into(),
                Bytes::from(task_id.as_str().to_string()),
            );
            self.store
                .put(&idx_key, Record { fields: idx_fields, ttl: None }, None)
                .await?;
        }

        // Publish JobCreated
        let all_task_ids: Vec<TaskId> = task_ids.iter().map(|(_, id)| id.clone()).collect();
        let payload = JobCreated {
            job_id: job_id.clone(),
            org_id: org_id.clone(),
            job_type: def.job_type.clone(),
            task_count: def.tasks.len() as u32,
            task_ids: all_task_ids,
            created_at: now,
            definition_ref: None,
        };
        let subject = subjects::jobs::created(&def.job_type);
        let json = serde_json::to_vec(&payload).map_err(|e| RunnerError::Other(e.to_string()))?;
        self.transport.publish(&subject, Bytes::from(json), None).await?;

        Ok(())
    }

    async fn dispatch_task(
        &self,
        task_id: &TaskId,
        job_id: &JobId,
        org_id: &OrgId,
        task_def: &TaskDefinition,
    ) -> Result<(), RunnerError> {
        let task_key = keys::task_key(task_def.task_type.as_str(), task_id.as_str());

        // CAS blocked -> pending (roots are already pending, skip CAS for them)
        let _ = self
            .store
            .compare_and_swap(
                &task_key,
                keys::fields::task::STATE,
                Bytes::from(TaskState::Blocked.as_str()),
                Bytes::from(TaskState::Pending.as_str()),
            )
            .await;

        // Publish TaskQueued
        let payload = TaskQueued {
            task_id: task_id.clone(),
            job_id: job_id.clone(),
            org_id: org_id.clone(),
            task_type: task_def.task_type.clone(),
            params: task_def.params.clone(),
            retry_count: 0,
        };
        let subject = subjects::tasks::queue(task_def.task_type.as_str());
        let json = serde_json::to_vec(&payload).map_err(|e| RunnerError::Other(e.to_string()))?;
        self.transport.publish(&subject, Bytes::from(json), None).await?;

        Ok(())
    }

    async fn complete_task(
        &self,
        task_id: &TaskId,
        job_id: &JobId,
        task_type: &TaskType,
    ) -> Result<(), RunnerError> {
        let now = now_ms();
        let task_key = keys::task_key(task_type.as_str(), task_id.as_str());

        let mut fields = HashMap::new();
        fields.insert(
            keys::fields::task::STATE.into(),
            Bytes::from(TaskState::Completed.as_str()),
        );
        fields.insert(keys::fields::task::UPDATED_AT.into(), Bytes::from(now.to_string()));
        self.store.set_fields(&task_key, fields).await?;

        // Increment job completed_count
        let payload = TaskCompleted {
            task_id: task_id.clone(),
            job_id: job_id.clone(),
            task_type: task_type.clone(),
            completed_at: now,
            result_ref: None,
        };
        let subject = subjects::tasks::terminal(task_type.as_str());
        let json = serde_json::to_vec(&payload).map_err(|e| RunnerError::Other(e.to_string()))?;
        self.transport.publish(&subject, Bytes::from(json), None).await?;

        Ok(())
    }

    async fn fail_task(
        &self,
        task_id: &TaskId,
        job_id: &JobId,
        task_type: &TaskType,
        error: &str,
    ) -> Result<(), RunnerError> {
        let now = now_ms();
        let task_key = keys::task_key(task_type.as_str(), task_id.as_str());

        let mut fields = HashMap::new();
        fields.insert(
            keys::fields::task::STATE.into(),
            Bytes::from(TaskState::Failed.as_str()),
        );
        fields.insert(keys::fields::task::ERROR.into(), Bytes::from(error.to_string()));
        fields.insert(keys::fields::task::UPDATED_AT.into(), Bytes::from(now.to_string()));
        self.store.set_fields(&task_key, fields).await?;

        let payload = TaskFailed {
            task_id: task_id.clone(),
            job_id: job_id.clone(),
            task_type: task_type.clone(),
            failed_at: now,
            error: error.to_string(),
            retry_count: 0,
            max_retries: 0,
        };
        let subject = subjects::tasks::terminal(task_type.as_str());
        let json = serde_json::to_vec(&payload).map_err(|e| RunnerError::Other(e.to_string()))?;
        self.transport.publish(&subject, Bytes::from(json), None).await?;

        Ok(())
    }

    async fn complete_job(
        &self,
        job_id: &JobId,
        org_id: &OrgId,
        job_type: &str,
    ) -> Result<(), RunnerError> {
        let now = now_ms();
        let job_key = keys::job_key(job_type, job_id.as_str());

        self.store
            .compare_and_swap(
                &job_key,
                keys::fields::job::STATE,
                Bytes::from(JobState::Running.as_str()),
                Bytes::from(JobState::Completed.as_str()),
            )
            .await?;

        let mut fields = HashMap::new();
        fields.insert(keys::fields::job::UPDATED_AT.into(), Bytes::from(now.to_string()));
        self.store.set_fields(&job_key, fields).await?;

        let payload = JobCompleted {
            job_id: job_id.clone(),
            org_id: org_id.clone(),
            job_type: job_type.to_string(),
            completed_at: now,
            result_ref: None,
        };
        let subject = subjects::jobs::completed(job_type);
        let json = serde_json::to_vec(&payload).map_err(|e| RunnerError::Other(e.to_string()))?;
        self.transport.publish(&subject, Bytes::from(json), None).await?;

        Ok(())
    }

    async fn fail_job(
        &self,
        job_id: &JobId,
        org_id: &OrgId,
        job_type: &str,
        failed_task_id: &TaskId,
        error: &str,
    ) -> Result<(), RunnerError> {
        let now = now_ms();
        let job_key = keys::job_key(job_type, job_id.as_str());

        self.store
            .compare_and_swap(
                &job_key,
                keys::fields::job::STATE,
                Bytes::from(JobState::Running.as_str()),
                Bytes::from(JobState::Failed.as_str()),
            )
            .await?;

        let mut fields = HashMap::new();
        fields.insert(keys::fields::job::ERROR.into(), Bytes::from(error.to_string()));
        fields.insert(keys::fields::job::UPDATED_AT.into(), Bytes::from(now.to_string()));
        self.store.set_fields(&job_key, fields).await?;

        let payload = JobFailed {
            job_id: job_id.clone(),
            org_id: org_id.clone(),
            job_type: job_type.to_string(),
            failed_at: now,
            failed_task_id: failed_task_id.clone(),
            error: error.to_string(),
        };
        let subject = subjects::jobs::failed(job_type);
        let json = serde_json::to_vec(&payload).map_err(|e| RunnerError::Other(e.to_string()))?;
        self.transport.publish(&subject, Bytes::from(json), None).await?;

        Ok(())
    }
}
