"""
Pytest fixtures for SmartSwitch tests.

Provides gNOI client with dsmsroot-signed TLS certificates for NPU gRPC
(port 50052). SmartSwitch devices use dsmsroot CA for client auth, not gnmiCA.

Runs grpcurl on the PTF container targeting the DUT management IP, consistent
with the standard PtfGrpc/PtfGnoi pattern used in grpc_fixtures.py.
"""
import logging
import os
import tempfile
import pytest

logger = logging.getLogger(__name__)

# SmartSwitch gNOI uses port 50052 (TLS) on NPU
SS_GNOI_PORT = 50052
SS_DUT_CERT_DIR = "/tmp/ss_gnoi_certs"
SS_PTF_CERT_DIR = "/tmp/ss_gnoi_certs_ptf"


def _create_dsmsroot_signed_client_certs(duthost, ptfhost):
    """
    Generate dsmsroot-signed client certs on DUT and distribute them to PTF.

    The dsmsroot CA key lives only on the DUT, so cert generation must happen
    there. After signing, the client cert and key are fetched from the DUT and
    copied to the PTF container so that grpcurl (running on PTF) can use them.

    SmartSwitch NPU gNOI server (port 50052) uses dsmsroot.cer as CA to verify
    client certs. gnmiCA-signed certs are rejected.

    Returns:
        Tuple of PTF-side paths: (client_cert_path, client_key_path)
    """
    duthost.shell(f"mkdir -p {SS_DUT_CERT_DIR}")

    # 1. Create client key
    duthost.shell(f"openssl genrsa -out {SS_DUT_CERT_DIR}/client.key 2048")
    # 2. Create CSR
    duthost.shell(
        f"openssl req -new -key {SS_DUT_CERT_DIR}/client.key "
        f"-out {SS_DUT_CERT_DIR}/client.csr -subj '/CN=grpc.client.sonic'"
    )
    # 3. Sign with dsmsroot (-set_serial avoids serial file permission issues)
    duthost.shell(
        f"sudo openssl x509 -req -in {SS_DUT_CERT_DIR}/client.csr "
        f"-CA /etc/sonic/telemetry/dsmsroot.cer "
        f"-CAkey /etc/sonic/telemetry/dsmsroot.key "
        f"-set_serial 1 -out {SS_DUT_CERT_DIR}/client.crt -days 365"
    )

    # Fetch certs from DUT to the test controller, then copy to PTF
    with tempfile.TemporaryDirectory() as tmp_dir:
        duthost.fetch(src=f"{SS_DUT_CERT_DIR}/client.crt", dest=tmp_dir, flat=True)
        duthost.fetch(src=f"{SS_DUT_CERT_DIR}/client.key", dest=tmp_dir, flat=True)

        ptfhost.shell(f"mkdir -p {SS_PTF_CERT_DIR}")
        ptfhost.copy(src=os.path.join(tmp_dir, "client.crt"), dest=f"{SS_PTF_CERT_DIR}/client.crt")
        ptfhost.copy(src=os.path.join(tmp_dir, "client.key"), dest=f"{SS_PTF_CERT_DIR}/client.key")

    return (
        f"{SS_PTF_CERT_DIR}/client.crt",
        f"{SS_PTF_CERT_DIR}/client.key",
    )


@pytest.fixture
def ptf_gnoi(ptfhost, duthost):
    """
    gNOI client for SmartSwitch with dsmsroot-signed TLS certs.

    Runs grpcurl on the PTF container targeting the DUT management IP on port
    50052. Client certs are generated on the DUT (where the dsmsroot CA key
    lives) and then distributed to PTF before use.
    """
    from tests.common.ptf_grpc import PtfGrpc
    from tests.common.ptf_gnoi import PtfGnoi

    ptf_cert, ptf_key = _create_dsmsroot_signed_client_certs(duthost, ptfhost)

    # Run grpcurl on PTF targeting DUT management IP
    target = f"{duthost.mgmt_ip}:{SS_GNOI_PORT}"
    client = PtfGrpc(
        ptfhost,
        target,
        plaintext=False,
        insecure=True,
    )
    client.configure_tls_certificates(
        ca_cert="",
        client_cert=ptf_cert,
        client_key=ptf_key,
    )
    client.configure_timeout(30.0)

    gnoi_client = PtfGnoi(client)
    logger.info(
        "Created SmartSwitch gNOI client: target=%s (on PTF), cert=%s",
        target,
        ptf_cert,
    )
    yield gnoi_client

    # Remove the client CN registered in CONFIG_DB during setup
    duthost.shell('sonic-db-cli CONFIG_DB del "GNMI_CLIENT_CERT|grpc.client.sonic"',
                  module_ignore_errors=True)

    # Remove client certs from DUT and PTF
    duthost.shell(f"rm -rf {SS_DUT_CERT_DIR}", module_ignore_errors=True)
    ptfhost.shell(f"rm -rf {SS_PTF_CERT_DIR}", module_ignore_errors=True)
    logger.info("Removed client cert directories on DUT and PTF")
