//! Stdio transport for JSON-RPC messages.

use crate::error::{McpError, ProtocolError, Result};
use crate::protocol::types::{JsonRpcRequest, JsonRpcResponse, Message};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Stdin, Stdout};
use tokio::sync::Mutex;
use tracing::{debug, error, trace};

/// Transport trait for MCP communication.
#[async_trait::async_trait]
pub trait Transport: Send + Sync {
    async fn read_message(&self) -> Result<Option<Message>>;
    async fn write_message(&self, message: &Message) -> Result<()>;
    async fn write_response(&self, response: &JsonRpcResponse) -> Result<()>;
}

/// Stdio-based transport for MCP.
pub struct StdioTransport {
    reader: Arc<Mutex<BufReader<Stdin>>>,
    writer: Arc<Mutex<Stdout>>,
}

impl StdioTransport {
    pub fn new() -> Self {
        Self {
            reader: Arc::new(Mutex::new(BufReader::new(tokio::io::stdin()))),
            writer: Arc::new(Mutex::new(tokio::io::stdout())),
        }
    }

    /// Read a single line from stdin.
    async fn read_line(&self) -> Result<Option<String>> {
        let mut reader = self.reader.lock().await;
        let mut line = String::new();

        match reader.read_line(&mut line).await {
            Ok(0) => Ok(None), // EOF
            Ok(_) => {
                let line = line.trim().to_string();
                if line.is_empty() {
                    Ok(None)
                } else {
                    trace!("Received line: {}", line);
                    Ok(Some(line))
                }
            }
            Err(e) => {
                error!("Error reading from stdin: {}", e);
                Err(McpError::Io(e))
            }
        }
    }

    /// Write a line to stdout.
    async fn write_line(&self, content: &str) -> Result<()> {
        let mut writer = self.writer.lock().await;
        trace!("Sending line: {}", content);
        writer.write_all(content.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        Ok(())
    }
}

impl Default for StdioTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Transport for StdioTransport {
    async fn read_message(&self) -> Result<Option<Message>> {
        let Some(line) = self.read_line().await? else {
            return Ok(None);
        };

        // Try to parse as request first, then as response
        match serde_json::from_str::<JsonRpcRequest>(&line) {
            Ok(request) => {
                debug!("Received request: method={}", request.method);
                Ok(Some(Message::Request(request)))
            }
            Err(_) => {
                // Try parsing as response
                match serde_json::from_str::<JsonRpcResponse>(&line) {
                    Ok(response) => {
                        debug!("Received response: id={:?}", response.id);
                        Ok(Some(Message::Response(response)))
                    }
                    Err(e) => {
                        error!("Failed to parse message: {}", e);
                        Err(McpError::Protocol(ProtocolError::ParseError))
                    }
                }
            }
        }
    }

    async fn write_message(&self, message: &Message) -> Result<()> {
        let json = serde_json::to_string(message)?;
        self.write_line(&json).await
    }

    async fn write_response(&self, response: &JsonRpcResponse) -> Result<()> {
        let json = serde_json::to_string(response)?;
        debug!("Sending response: id={:?}", response.id);
        self.write_line(&json).await
    }
}

/// Message reader that continuously reads from transport.
pub struct MessageReader<T: Transport> {
    transport: Arc<T>,
}

impl<T: Transport> MessageReader<T> {
    pub fn new(transport: Arc<T>) -> Self {
        Self { transport }
    }

    /// Read the next message from the transport.
    pub async fn next(&self) -> Result<Option<Message>> {
        self.transport.read_message().await
    }
}

/// Message writer for sending responses.
pub struct MessageWriter<T: Transport> {
    transport: Arc<T>,
}

impl<T: Transport> MessageWriter<T> {
    pub fn new(transport: Arc<T>) -> Self {
        Self { transport }
    }

    /// Send a response.
    pub async fn send(&self, response: JsonRpcResponse) -> Result<()> {
        self.transport.write_response(&response).await
    }

    /// Send a message.
    pub async fn send_message(&self, message: &Message) -> Result<()> {
        self.transport.write_message(message).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::RequestId;

    #[test]
    fn test_request_parsing() {
        let json = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#;
        let request: JsonRpcRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.method, "initialize");
        assert_eq!(request.id, Some(RequestId::Number(1)));
    }

    #[test]
    fn test_response_parsing() {
        let json = r#"{"jsonrpc":"2.0","id":1,"result":{"test":true}}"#;
        let response: JsonRpcResponse = serde_json::from_str(json).unwrap();
        assert!(response.result.is_some());
        assert!(response.error.is_none());
    }
}
