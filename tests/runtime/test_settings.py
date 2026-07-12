# tests/runtime/test_settings.py
from urbanpulse.runtime.settings import RuntimeSettings


def test_local_defaults_match_compose_host_endpoints() -> None:
    settings = RuntimeSettings(_env_file=None)

    assert settings.kafka_bootstrap_servers == "localhost:19092"
    assert settings.postgres_dsn == (
        "postgresql://urbanpulse:urbanpulse-local-only@localhost:55433/urbanpulse"
    )
    assert settings.object_store_endpoint_url == "http://localhost:18333"
    assert settings.object_store_bucket == "urbanpulse-raw"

def test_prefixed_environment_variables_override_local_defaults(
    monkeypatch,
) -> None:
    monkeypatch.setenv("URBANPULSE_KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
    monkeypatch.setenv("URBANPULSE_OBJECT_STORE_BUCKET", "alternate-raw")

    settings = RuntimeSettings(_env_file=None)

    assert settings.kafka_bootstrap_servers == "kafka:19092"
    assert settings.object_store_bucket == "alternate-raw"
