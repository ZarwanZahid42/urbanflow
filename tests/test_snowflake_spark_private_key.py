import pytest
from cryptography.hazmat.primitives import serialization

from notebooks.utilities.snowflake_common import (
    SnowflakeConfig,
    normalize_snowflake_spark_private_key,
    snowflake_spark_options,
)


def config() -> SnowflakeConfig:
    return SnowflakeConfig(account="xy12345.us-east-1", user="URBANFLOW_DATABRICKS_SVC")


@pytest.mark.parametrize("line_ending", ["\n", "\r\n", "escaped_lf", "escaped_crlf"])
def test_pkcs8_pem_line_endings_are_normalized(
    synthetic_pkcs8_pem: str,
    synthetic_pkcs8_payload: str,
    line_ending: str,
):
    if line_ending == "escaped_lf":
        supplied_key = synthetic_pkcs8_pem.replace("\n", r"\n")
    elif line_ending == "escaped_crlf":
        supplied_key = synthetic_pkcs8_pem.replace("\n", r"\r\n")
    else:
        supplied_key = synthetic_pkcs8_pem.replace("\n", line_ending)

    assert normalize_snowflake_spark_private_key(supplied_key) == synthetic_pkcs8_payload


def test_surrounding_whitespace_is_removed(
    synthetic_pkcs8_pem: str,
    synthetic_pkcs8_payload: str,
):
    supplied_key = f" \r\n\t{synthetic_pkcs8_pem}\r\n "
    assert normalize_snowflake_spark_private_key(supplied_key) == synthetic_pkcs8_payload


@pytest.mark.parametrize("supplied_key", ["", " ", "\r\n"])
def test_empty_key_is_rejected_without_echoing_input(supplied_key: str):
    with pytest.raises(ValueError, match="empty") as exc_info:
        normalize_snowflake_spark_private_key(supplied_key)
    assert str(exc_info.value) == "Snowflake private key secret is empty"


def test_encrypted_pkcs8_key_is_rejected(synthetic_private_key):
    encrypted_pem = synthetic_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(b"synthetic-passphrase"),
    ).decode("ascii")
    with pytest.raises(ValueError, match="unencrypted") as exc_info:
        normalize_snowflake_spark_private_key(encrypted_pem)
    assert encrypted_pem not in str(exc_info.value)


def test_rsa_pkcs1_key_is_rejected(synthetic_private_key):
    pkcs1_pem = synthetic_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")
    with pytest.raises(ValueError, match="PKCS#8") as exc_info:
        normalize_snowflake_spark_private_key(pkcs1_pem)
    assert pkcs1_pem not in str(exc_info.value)


def test_spark_options_contain_only_normalized_key_payload_and_do_not_print_it(
    synthetic_pkcs8_pem: str,
    synthetic_pkcs8_payload: str,
    capsys: pytest.CaptureFixture[str],
):
    options = snowflake_spark_options(config(), synthetic_pkcs8_pem, schema="LANDING")
    assert options["pem_private_key"] == synthetic_pkcs8_payload
    assert "BEGIN PRIVATE KEY" not in options["pem_private_key"]
    assert "END PRIVATE KEY" not in options["pem_private_key"]
    assert not any(character.isspace() for character in options["pem_private_key"])
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
