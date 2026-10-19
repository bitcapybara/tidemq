use std::sync::Arc;

use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::{ServerConfig, crypto::aws_lc_rs, pki_types::PrivatePkcs8KeyDer};

pub fn untrusted_acceptor() -> TlsAcceptor {
    let key = rcgen::KeyPair::generate().unwrap();
    let params = rcgen::CertificateParams::new(vec!["127.0.0.1".into()]).unwrap();
    let certificate = params.self_signed(&key).unwrap();
    let config = ServerConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![certificate.der().clone()],
            PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )
        .unwrap();
    TlsAcceptor::from(Arc::new(config))
}
