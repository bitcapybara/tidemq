use super::*;
use crate::runtime::wire::semantic_local_tests::{Fixture, TOKEN};

#[tokio::test]
async fn local_authentication_and_scope_authorization_do_not_require_remote_encoding() {
    let fixture = Fixture::new().await;
    fixture.service.auth_cache.0.lock().unwrap().clear();
    let result = wire::without_remote_encoding(async {
        let deadline = Instant::now() + Duration::from_secs(3);
        let (authenticated, _) = fixture
            .service
            .auth_query(
                AuthQuery::Authenticate(TokenDigest::of(TOKEN).unwrap()),
                deadline,
            )
            .await?;
        assert_eq!(authenticated.credential, Some(fixture.credential));
        let (authorized, expires) = fixture
            .service
            .auth_query(
                AuthQuery::Authorize {
                    credential: fixture.credential,
                    permission: Permission::Inspect,
                    resource: AuthResource::Topic(fixture.partition.topic_id),
                },
                deadline,
            )
            .await?;
        assert_eq!(authorized.credential, Some(fixture.credential));
        assert!(expires > Instant::now());
        let (birth, _) = fixture
            .service
            .auth_query(AuthQuery::BirthShard(fixture.shard), deadline)
            .await?;
        assert_eq!(birth.birth.unwrap().route.id, fixture.shard);
        Ok::<_, Error>(())
    })
    .await;
    fixture.close().await;
    result.unwrap();
}
