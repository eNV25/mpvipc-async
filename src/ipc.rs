//! IPC handling thread/task. Handles communication between [`Mpv`](crate::Mpv) instances and mpv's unix socket

use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::{
    net::UnixStream,
    sync::{broadcast, mpsc, oneshot},
};
use tokio_util::codec::{Framed, LinesCodec};

use crate::MpvError;

/// Container for all state that regards communication with the mpv IPC socket
/// and message passing with [`Mpv`](crate::Mpv) controllers.
pub(crate) struct MpvIpc {
    socket: Framed<UnixStream, LinesCodec>,
    command_channel: mpsc::Receiver<(MpvIpcCommand, oneshot::Sender<MpvIpcResponse>)>,
    event_channel: broadcast::Sender<MpvIpcEvent>,
}

/// Commands that can be sent to [`MpvIpc`]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MpvIpcCommand {
    Command(Vec<String>),
    GetProperty(String),
    SetProperty(String, Value),
    ObserveProperty(u64, String),
    UnobserveProperty(u64),
    Exit,
}

/// [`MpvIpc`]'s response to a [`MpvIpcCommand`].
#[derive(Debug)]
pub(crate) struct MpvIpcResponse(pub(crate) Result<Option<Value>, MpvError>);

/// A deserialized and partially parsed event from mpv.
#[derive(Debug, Clone)]
pub(crate) struct MpvIpcEvent(pub(crate) Value);

impl MpvIpc {
    pub(crate) fn new(
        socket: UnixStream,
        command_channel: mpsc::Receiver<(MpvIpcCommand, oneshot::Sender<MpvIpcResponse>)>,
        event_channel: broadcast::Sender<MpvIpcEvent>,
    ) -> Self {
        MpvIpc {
            socket: Framed::new(socket, LinesCodec::new()),
            command_channel,
            event_channel,
        }
    }

    pub(crate) async fn send_command(
        &mut self,
        command: &[Value],
    ) -> Result<Option<Value>, MpvError> {
        let ipc_command = json!({ "command": command });
        let ipc_command_str =
            serde_json::to_string(&ipc_command).map_err(MpvError::JsonParseError)?;

        log::trace!("Sending command: {}", ipc_command_str);

        self.socket
            .send(ipc_command_str)
            .await
            .map_err(|why| MpvError::MpvSocketConnectionError(why.to_string()))?;

        let response = loop {
            let response = self
                .socket
                .next()
                .await
                .ok_or(MpvError::MpvSocketConnectionError(
                    "Could not receive response from mpv".to_owned(),
                ))?
                .map_err(|why| MpvError::MpvSocketConnectionError(why.to_string()))?;

            let parsed_response =
                serde_json::from_str::<Value>(&response).map_err(MpvError::JsonParseError);

            if parsed_response
                .as_ref()
                .ok()
                .and_then(|v| v.as_object().map(|o| o.contains_key("event")))
                .unwrap_or(false)
            {
                self.handle_event(parsed_response).await;
            } else {
                break parsed_response;
            }
        };

        log::trace!("Received response: {:?}", response);

        parse_mpv_response_data(response?, command)
    }

    pub(crate) async fn get_mpv_property(
        &mut self,
        property: &str,
    ) -> Result<Option<Value>, MpvError> {
        self.send_command(&[json!("get_property"), json!(property)])
            .await
    }

    pub(crate) async fn set_mpv_property(
        &mut self,
        property: &str,
        value: Value,
    ) -> Result<Option<Value>, MpvError> {
        self.send_command(&[json!("set_property"), json!(property), value])
            .await
    }

    pub(crate) async fn observe_property(
        &mut self,
        id: u64,
        property: &str,
    ) -> Result<Option<Value>, MpvError> {
        self.send_command(&[json!("observe_property"), json!(id), json!(property)])
            .await
    }

    pub(crate) async fn unobserve_property(&mut self, id: u64) -> Result<Option<Value>, MpvError> {
        self.send_command(&[json!("unobserve_property"), json!(id)])
            .await
    }

    async fn handle_event(&mut self, event: Result<Value, MpvError>) {
        match &event {
            Ok(event) => {
                log::trace!("Parsed event: {:?}", event);
                if let Err(broadcast::error::SendError(_)) =
                    self.event_channel.send(MpvIpcEvent(event.to_owned()))
                {
                    log::trace!("Failed to send event to channel, ignoring");
                }
            }
            Err(e) => {
                log::trace!("Error parsing event, ignoring:\n  {:?}\n  {:?}", &event, e);
            }
        }
    }

    pub(crate) async fn run(mut self) -> Result<(), MpvError> {
        loop {
            tokio::select! {
              event = self.socket.next() => {
                let Some(event) = event else {
                    if let Err(broadcast::error::SendError(_)) =
                        self.event_channel.send(MpvIpcEvent(json!({ "event": "shutdown" })))
                    {
                        log::trace!("Failed to send shutdown event to channel, ignoring");
                    };
                    return Ok(());
                };
                log::trace!("Got event: {:?}", event);

                let parsed_event = event
                    .map_err(|why| MpvError::MpvSocketConnectionError(why.to_string()))
                    .and_then(|event|
                        serde_json::from_str::<Value>(&event)
                        .map_err(MpvError::JsonParseError));

                self.handle_event(parsed_event).await;
              }
              Some((cmd, tx)) = self.command_channel.recv() => {
                  log::trace!("Handling command: {:?}", cmd);
                  match cmd {
                      MpvIpcCommand::Command(command) => {
                          let refs = command.iter().map(|s| json!(s)).collect::<Vec<Value>>();
                          let response = self.send_command(refs.as_slice()).await;
                          tx.send(MpvIpcResponse(response)).unwrap()
                      }
                      MpvIpcCommand::GetProperty(property) => {
                          let response = self.get_mpv_property(&property).await;
                          tx.send(MpvIpcResponse(response)).unwrap()
                      }
                      MpvIpcCommand::SetProperty(property, value) => {
                          let response = self.set_mpv_property(&property, value).await;
                          tx.send(MpvIpcResponse(response)).unwrap()
                      }
                      MpvIpcCommand::ObserveProperty(id, property) => {
                          let response = self.observe_property(id, &property).await;
                          tx.send(MpvIpcResponse(response)).unwrap()
                      }
                      MpvIpcCommand::UnobserveProperty(id) => {
                          let response = self.unobserve_property(id).await;
                          tx.send(MpvIpcResponse(response)).unwrap()
                      }
                      MpvIpcCommand::Exit => {
                        tx.send(MpvIpcResponse(Ok(None))).unwrap();
                        return Ok(());
                      }
                  }
              }
            }
        }
    }
}

/// This function does the most basic JSON parsing and error handling
/// for status codes and errors that all responses from mpv are
/// expected to contain.
fn parse_mpv_response_data(value: Value, command: &[Value]) -> Result<Option<Value>, MpvError> {
    log::trace!("Parsing mpv response data: {:?}", value);
    let result = value
        .as_object()
        .ok_or(MpvError::ValueContainsUnexpectedType {
            expected_type: "object".to_string(),
            received: value.clone(),
        })
        .and_then(|o| {
            let error = o
                .get("error")
                .ok_or(MpvError::MissingKeyInObject {
                    key: "error".to_string(),
                    map: o.clone(),
                })?
                .as_str()
                .ok_or(MpvError::ValueContainsUnexpectedType {
                    expected_type: "string".to_string(),
                    received: o.get("error").unwrap().clone(),
                })?;

            let data = o.get("data");

            Ok((error, data))
        })
        .and_then(|(error, data)| match error {
            "success" => Ok(data),
            "property unavailable" => Ok(None),
            err => Err(MpvError::MpvError {
                command: command.to_owned(),
                message: err.to_string(),
            }),
        });

    match &result {
        Ok(v) => log::trace!("Successfully parsed mpv response data: {:?}", v),
        Err(e) => log::trace!("Error parsing mpv response data: {:?}", e),
    }

    result.map(|opt| opt.cloned())
}
