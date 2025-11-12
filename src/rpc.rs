use futures::prelude::*;

#[trpc::service]
pub trait RaftRpc {
    async fn echo(name: String) -> String;
}
