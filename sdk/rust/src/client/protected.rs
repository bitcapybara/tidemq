//! A logical consumer owns its native registration across Delivery reconnects.
use super::*;
use tidemq_transport::rpc::ProtectedCapacity;

#[derive(Default)]
pub(crate) struct NativeProtection {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    capacity: Option<ProtectedCapacity>,
    rpc: Option<RpcClient>,
}

impl NativeProtection {
    pub async fn register(&self, client: &Client, deadline: Instant) -> Result<()> {
        let mut state = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| Error::Deadline)?;
        if state.capacity.is_none() {
            let rpc = client.rpc(TrafficClass::Delivery, deadline).await?;
            state.capacity = Some(rpc.reserve_protected()?);
        }
        Ok(())
    }

    pub async fn rpc(&self, client: &Client, deadline: Instant) -> Result<RpcClient> {
        let mut state = timeout_at(deadline, self.state.lock())
            .await
            .map_err(|_| Error::Deadline)?;
        if let Some(rpc) = state.rpc.as_ref().filter(|rpc| !rpc.is_closed()) {
            return Ok(rpc.clone());
        }
        if let Some(old) = state.rpc.take() {
            old.shutdown(deadline).await;
            // The old private lane guard must drop before its replacement can open.
            drop(old);
        }
        let capacity = state.capacity.as_ref().ok_or(Error::Closed)?;
        let ordinary = client.rpc(TrafficClass::Delivery, deadline).await?;
        let rpc = ordinary.open_protected(capacity, deadline).await?;
        state.rpc = Some(rpc.clone());
        Ok(rpc)
    }

    pub async fn unregister(&self, deadline: Instant) -> Result<()> {
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(_) => timeout_at(deadline, self.state.lock())
                .await
                .map_err(|_| Error::Deadline)?,
        };
        if let Some(rpc) = state.rpc.take() {
            rpc.shutdown(deadline).await;
            drop(rpc);
        }
        state.capacity.take();
        Ok(())
    }
}
