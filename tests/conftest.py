import httpx
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-network",
        action="store_true",
        default=False,
        help="Run tests that require network access",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-network"):
        return
    skip_network = pytest.mark.skip(reason="network test (enable with --run-network)")
    for item in items:
        if "network" in item.keywords:
            item.add_marker(skip_network)


@pytest.fixture(autouse=True)
def _disable_network(monkeypatch, request):
    if request.node.get_closest_marker("network"):
        return

    def _blocked(*args, **kwargs):
        raise RuntimeError("Network disabled in tests. Mock httpx.Client.get().")

    monkeypatch.setattr(httpx.Client, "get", _blocked)
