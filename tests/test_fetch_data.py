import pytest

from scripts import fetch_data


def cfg():
    return {"prometheus": {}}


def test_local_token_from_environment(capture, monkeypatch, tmp_path):
    monkeypatch.setenv("SYSDIG_MONITOR_API_TOKEN", "from-env")
    env_file = tmp_path / ".env"
    env_file.write_text("SYSDIG_MONITOR_API_TOKEN=from-file\n")

    c = cfg()
    fetch_data.inject_local_bearer_token(c, capture.logger, env_file)
    assert c["prometheus"]["bearer_token"] == "from-env"


def test_local_token_from_env_file(capture, monkeypatch, tmp_path):
    monkeypatch.delenv("SYSDIG_MONITOR_API_TOKEN", raising=False)
    env_file = tmp_path / "local.env"
    env_file.write_text('# comment\n\nOTHER=x\nexport SYSDIG_MONITOR_API_TOKEN="from-file"\n')

    c = cfg()
    fetch_data.inject_local_bearer_token(c, capture.logger, env_file)
    assert c["prometheus"]["bearer_token"] == "from-file"


def test_local_token_missing_raises(capture, monkeypatch, tmp_path):
    monkeypatch.delenv("SYSDIG_MONITOR_API_TOKEN", raising=False)

    with pytest.raises(ValueError, match="SYSDIG_MONITOR_API_TOKEN"):
        fetch_data.inject_local_bearer_token(cfg(), capture.logger, tmp_path / "missing.env")


def test_local_mode_does_not_call_aws(monkeypatch, tmp_path):
    monkeypatch.setenv("SYSDIG_MONITOR_API_TOKEN", "from-env")
    monkeypatch.setattr(fetch_data, "get_secret", lambda *a, **k: pytest.fail("AWS Secrets Manager was called"))
    config_file = tmp_path / "config.yaml"
    config_file.write_text("prometheus:\n  url: https://example.test\n")

    c = fetch_data.load_config(str(config_file), local=True)
    assert c["prometheus"]["bearer_token"] == "from-env"


def test_parse_args():
    assert fetch_data.parse_args([]).local is False
    args = fetch_data.parse_args(["--local", "--env-file", "x.env"])
    assert args.local and args.env_file == "x.env"
    with pytest.raises(SystemExit):
        fetch_data.parse_args(["--env-file", "x.env"])
