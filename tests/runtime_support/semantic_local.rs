use std::sync::Arc;

use crate::config::Role;
use crate::metadata::{PartitionKey, TokenDigest};
use crate::runtime::{data::consumer_tests::fixture, service::Service};
use tidemq_protocol::Id;

pub(in crate::runtime) const TOKEN: &str = "runtime-test-access-token-with-at-least-32-characters";

pub(in crate::runtime) struct Fixture {
    pub runtime: fixture::Fixture,
    pub client: tidemq_sdk::Client,
    pub service: Arc<Service>,
    pub credential: Id,
    pub partition: PartitionKey,
    pub shard: Id,
}

impl Fixture {
    pub async fn new() -> Self {
        let mut runtime = fixture::Fixture::new(&[(1, Role::Combined)], &[1], &[1]).await;
        runtime.start_all().await;
        let service = runtime.running.get(&1).unwrap().service.clone();
        let client = runtime.client(1).await;
        let tenant = client.admin().create_tenant("typed").await.unwrap();
        client
            .admin()
            .create_namespace(tenant, "app")
            .await
            .unwrap();
        let topic = client
            .admin()
            .create_topic("typed/app/events")
            .await
            .unwrap();
        let topic: Id = topic.id().into();
        let (credential, shard) = service
            .metadata
            .as_ref()
            .unwrap()
            .state
            .try_with_application(|state| {
                (
                    state
                        .auth()
                        .authenticate(
                            TokenDigest::of(TOKEN).unwrap(),
                            crate::runtime::data::unix_millis().unwrap(),
                        )
                        .unwrap()
                        .id,
                    state.topology().domains(topic).next().unwrap().route.id,
                )
            })
            .await
            .unwrap();
        Self {
            runtime,
            client,
            service,
            credential,
            partition: PartitionKey {
                topic_id: topic,
                partition: 0,
            },
            shard,
        }
    }

    pub async fn close(self) {
        self.client.close().await.unwrap();
        self.runtime.close().await;
    }
}
