use async_trait::async_trait;
use gbe_jobs_domain::{TaskDefinition, TaskOutcome, TaskType};
use reqwest::{Client, Method};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info};

use crate::operative::{Operative, OperativeError};

const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// HTTP operative: makes structured HTTP requests from task params.
///
/// Params:
/// - `url` (required): target URL
/// - `method` (optional): GET, POST, PUT, DELETE — defaults to GET
/// - `headers` (optional): JSON object string of header key-value pairs
/// - `body` (optional): request body string
///
/// Uses `task.timeout_secs` if set, otherwise 30s default.
///
/// On 2xx: returns Completed with structured data containing status, headers, body.
/// On non-2xx: returns Failed with HTTP status code as exit_code.
pub struct HttpOperative {
    task_types: Vec<TaskType>,
    client: Client,
}

impl HttpOperative {
    pub fn new(task_types: Vec<TaskType>) -> Self {
        Self {
            task_types,
            client: Client::new(),
        }
    }

    pub fn for_types(types: &[&str]) -> Result<Self, gbe_jobs_domain::JobsDomainError> {
        let task_types: Result<Vec<TaskType>, _> =
            types.iter().map(|t| TaskType::new(t)).collect();
        Ok(Self::new(task_types?))
    }
}

fn parse_method(s: &str) -> Result<Method, OperativeError> {
    match s.to_uppercase().as_str() {
        "GET" => Ok(Method::GET),
        "POST" => Ok(Method::POST),
        "PUT" => Ok(Method::PUT),
        "DELETE" => Ok(Method::DELETE),
        "PATCH" => Ok(Method::PATCH),
        "HEAD" => Ok(Method::HEAD),
        other => Err(OperativeError::Execution(format!(
            "unsupported HTTP method: {other}"
        ))),
    }
}

fn parse_headers(json_str: &str) -> Result<HashMap<String, String>, OperativeError> {
    serde_json::from_str::<HashMap<String, String>>(json_str).map_err(|e| {
        OperativeError::Execution(format!("invalid headers JSON: {e}"))
    })
}

#[async_trait]
impl Operative for HttpOperative {
    fn handles(&self) -> &[TaskType] {
        &self.task_types
    }

    async fn execute(&self, task: &TaskDefinition) -> Result<TaskOutcome, OperativeError> {
        let url = task
            .params
            .entries
            .get("url")
            .ok_or_else(|| OperativeError::MissingParam("url".to_string()))?;

        let method = match task.params.entries.get("method") {
            Some(m) => parse_method(m)?,
            None => Method::GET,
        };

        let timeout = Duration::from_secs(
            task.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS),
        );

        debug!(task = %task.name, %url, %method, "executing HTTP request");

        let mut req = self.client.request(method.clone(), url).timeout(timeout);

        if let Some(headers_json) = task.params.entries.get("headers") {
            for (k, v) in parse_headers(headers_json)? {
                req = req.header(&k, &v);
            }
        }

        if let Some(body) = task.params.entries.get("body") {
            req = req.body(body.clone());
        }

        let response = req.send().await.map_err(|e| {
            OperativeError::Execution(format!("HTTP request failed: {e}"))
        })?;

        let status = response.status().as_u16();
        let response_headers: serde_json::Value = serde_json::Value::Object(
            response
                .headers()
                .iter()
                .map(|(k, v)| {
                    (
                        k.to_string(),
                        serde_json::Value::String(
                            v.to_str().unwrap_or("<binary>").to_string(),
                        ),
                    )
                })
                .collect(),
        );

        let body_text = response.text().await.map_err(|e| {
            OperativeError::Execution(format!("failed to read response body: {e}"))
        })?;

        if (200..300).contains(&status) {
            info!(task = %task.name, status, "HTTP request completed");

            let body_value = match serde_json::from_str::<serde_json::Value>(&body_text) {
                Ok(v) => v,
                Err(_) => serde_json::Value::String(body_text.clone()),
            };

            let data = serde_json::json!({
                "status": status,
                "headers": response_headers,
                "body": body_value,
            });

            let output: Vec<String> = body_text.lines().map(String::from).collect();

            Ok(TaskOutcome::Completed {
                output,
                result_ref: None,
                data: Some(data),
            })
        } else {
            info!(task = %task.name, status, "HTTP request returned non-2xx");
            Ok(TaskOutcome::Failed {
                exit_code: status as i32,
                error: body_text,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gbe_jobs_domain::TaskParams;
    use wiremock::matchers::{body_string, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn http_task(name: &str, params: Vec<(&str, &str)>) -> TaskDefinition {
        let mut tp = TaskParams::default();
        for (k, v) in params {
            tp.entries.insert(k.to_string(), v.to_string());
        }
        TaskDefinition {
            name: name.to_string(),
            task_type: TaskType::new("http").unwrap(),
            depends_on: vec![],
            params: tp,
            input_from: HashMap::new(),
            timeout_secs: None,
            max_retries: None,
        }
    }

    #[tokio::test]
    async fn missing_url_errors() {
        let op = HttpOperative::for_types(&["http"]).unwrap();
        let task = http_task("no-url", vec![("method", "GET")]);
        let err = op.execute(&task).await.unwrap_err();
        assert!(matches!(err, OperativeError::MissingParam(_)));
    }

    #[tokio::test]
    async fn invalid_method_errors() {
        let op = HttpOperative::for_types(&["http"]).unwrap();
        let task = http_task("bad-method", vec![
            ("url", "http://localhost"),
            ("method", "YEET"),
        ]);
        let err = op.execute(&task).await.unwrap_err();
        assert!(err.to_string().contains("unsupported HTTP method"), "got: {err}");
    }

    #[tokio::test]
    async fn get_json_response() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/data"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"name": "test", "value": 42})),
            )
            .mount(&server)
            .await;

        let op = HttpOperative::for_types(&["http"]).unwrap();
        let task = http_task("get-json", vec![
            ("url", &format!("{}/data", server.uri())),
        ]);

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Completed { data, .. } => {
                let data = data.unwrap();
                assert_eq!(data["status"], 200);
                assert_eq!(data["body"]["name"], "test");
                assert_eq!(data["body"]["value"], 42);
            }
            TaskOutcome::Failed { .. } => panic!("expected Completed"),
        }
    }

    #[tokio::test]
    async fn post_with_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/submit"))
            .and(body_string("hello world"))
            .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
            .mount(&server)
            .await;

        let op = HttpOperative::for_types(&["http"]).unwrap();
        let task = http_task("post-body", vec![
            ("url", &format!("{}/submit", server.uri())),
            ("method", "POST"),
            ("body", "hello world"),
        ]);

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Completed { output, .. } => {
                assert_eq!(output, vec!["ok"]);
            }
            TaskOutcome::Failed { .. } => panic!("expected Completed"),
        }
    }

    #[tokio::test]
    async fn non_2xx_returns_failed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/missing"))
            .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
            .mount(&server)
            .await;

        let op = HttpOperative::for_types(&["http"]).unwrap();
        let task = http_task("get-404", vec![
            ("url", &format!("{}/missing", server.uri())),
        ]);

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Failed { exit_code, error } => {
                assert_eq!(exit_code, 404);
                assert_eq!(error, "not found");
            }
            TaskOutcome::Completed { .. } => panic!("expected Failed"),
        }
    }

    #[tokio::test]
    async fn headers_forwarded() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/auth"))
            .and(header("authorization", "Bearer tok123"))
            .respond_with(ResponseTemplate::new(200).set_body_string("authorized"))
            .mount(&server)
            .await;

        let op = HttpOperative::for_types(&["http"]).unwrap();
        let task = http_task("with-headers", vec![
            ("url", &format!("{}/auth", server.uri())),
            ("headers", r#"{"authorization":"Bearer tok123"}"#),
        ]);

        let outcome = op.execute(&task).await.unwrap();
        match outcome {
            TaskOutcome::Completed { output, .. } => {
                assert_eq!(output, vec!["authorized"]);
            }
            TaskOutcome::Failed { .. } => panic!("expected Completed"),
        }
    }

    #[tokio::test]
    async fn timeout_returns_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/slow"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("too late")
                    .set_delay(Duration::from_secs(5)),
            )
            .mount(&server)
            .await;

        let op = HttpOperative::for_types(&["http"]).unwrap();
        let mut task = http_task("timeout", vec![
            ("url", &format!("{}/slow", server.uri())),
        ]);
        task.timeout_secs = Some(1);

        let err = op.execute(&task).await.unwrap_err();
        assert!(err.to_string().contains("request failed"), "got: {err}");
    }
}
