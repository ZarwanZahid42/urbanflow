import re

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


@pytest.fixture(scope="session")
def synthetic_private_key():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="session")
def synthetic_pkcs8_pem(synthetic_private_key) -> str:
    return synthetic_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")


@pytest.fixture(scope="session")
def synthetic_pkcs8_payload(synthetic_pkcs8_pem: str) -> str:
    return re.sub(
        r"\s+",
        "",
        synthetic_pkcs8_pem.removeprefix("-----BEGIN PRIVATE KEY-----").removesuffix(
            "-----END PRIVATE KEY-----\n"
        ),
    )
