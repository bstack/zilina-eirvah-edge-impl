from eirvah_observability.health import HealthApp
from eirvah_observability.metrics import make_counter
from prometheus_client import CollectorRegistry
from starlette.testclient import TestClient


def _readiness_true() -> bool:
    return True


def _readiness_false() -> bool:
    return False


def test_healthz_returns_200_when_alive() -> None:
    app = HealthApp(is_ready=_readiness_true)
    client = TestClient(app.asgi)
    assert client.get("/healthz").status_code == 200


def test_readyz_returns_200_when_ready() -> None:
    app = HealthApp(is_ready=_readiness_true)
    client = TestClient(app.asgi)
    assert client.get("/readyz").status_code == 200


def test_readyz_returns_503_when_not_ready() -> None:
    app = HealthApp(is_ready=_readiness_false)
    client = TestClient(app.asgi)
    assert client.get("/readyz").status_code == 503


def test_metrics_serves_prometheus_exposition() -> None:
    reg = CollectorRegistry()
    c = make_counter("health_app_test", "doc", labelnames=[], registry=reg)
    c.inc()
    app = HealthApp(is_ready=_readiness_true, registry=reg)
    client = TestClient(app.asgi)
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "eirvah_health_app_test_total" in resp.text


def test_unknown_path_returns_404() -> None:
    app = HealthApp(is_ready=_readiness_true)
    client = TestClient(app.asgi)
    assert client.get("/whatever").status_code == 404
