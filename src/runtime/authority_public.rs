use tidemq_protocol::{
    Id,
    common::{Frame, FrameKind},
    framing::decode_payload,
    public,
};
use tokio::time::Instant;

use super::{
    Error, Result,
    service::{Service, forbidden},
    wire,
};
use crate::{
    gateway::Access,
    metadata::{AuthorityTransfer, Permission, TransferStage},
};

impl Service {
    pub(super) async fn authority_result(&self, id: Id) -> Result<public::ClusterControlResult> {
        let plan = self
            .metadata
            .as_ref()
            .ok_or_else(Error::unavailable)?
            .state
            .try_with_application(|state| state.topology().transfer(id).cloned())
            .await
            .map_err(|source| Error::operation("read authority transfer receipt", source))?
            .ok_or_else(Error::unavailable)?;
        Ok(public::ClusterControlResult {
            authority_transfer: Some(transfer_info(plan)),
            ..Default::default()
        })
    }

    pub(super) async fn authority_status_public(
        &self,
        access: &Access,
        frame: &Frame,
        deadline: Instant,
    ) -> Result<Frame> {
        let actor: Id = access
            .principal
            .strip_prefix("credential:")
            .ok_or_else(forbidden)?
            .parse()
            .map_err(|_| forbidden())?;
        let request: public::ManageCluster =
            decode_payload(frame.payload.clone(), &self.config.protocol).map_err(|source| {
                Error::operation("decode authority transfer status query", source)
            })?;
        let id = wire::id(&request.transfer_operation)?;
        let replica = self.metadata.as_ref().ok_or_else(Error::unavailable)?;
        replica
            .group
            .ensure_read(deadline)
            .await
            .map_err(crate::runtime::wire::execution_error)?;
        let now = super::data::unix_millis()?;
        let plan = replica
            .state
            .try_with_application(|state| {
                let principal = state
                    .auth()
                    .authorize(actor, now, Permission::ManageTenants, None)
                    .map_err(|_| forbidden())?;
                if principal.tenant.is_some() {
                    return Err(forbidden());
                }
                Ok(state.topology().transfer(id).cloned())
            })
            .await
            .map_err(|source| Error::operation("authorize authority transfer status", source))??;
        let reply = public::ClusterControlResult {
            authority_transfer: plan.map(transfer_info),
            ..Default::default()
        };
        wire::response(
            frame,
            FrameKind::ClusterControlResult,
            &reply,
            &self.config.protocol,
        )
    }
}

fn transfer_info(plan: AuthorityTransfer) -> public::AuthorityTransferInfo {
    public::AuthorityTransferInfo {
        operation_id: super::metadata::bytes_id(plan.id),
        shard_id: super::metadata::bytes_id(plan.shard),
        topic_id: super::metadata::bytes_id(plan.partition.topic_id),
        source_group: plan.source.group_id,
        target_group: plan.target.group_id,
        stage: match plan.stage {
            TransferStage::Prepare => 0,
            TransferStage::Frozen => 1,
            TransferStage::Exported => 2,
            TransferStage::Imported => 3,
            TransferStage::Published => 4,
            TransferStage::Checkpointed => 5,
            TransferStage::Retired => 6,
        },
    }
}
