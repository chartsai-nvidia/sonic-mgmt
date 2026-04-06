import ipaddress
import logging
import os
import time
import json
import pytest

from tests.common.helpers.assertions import pytest_assert

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.topology("t0", "t1", "any"),
    pytest.mark.device_type("vs"),
]

ROUTE_COUNT = int(os.environ.get("STATIC_ROUTE_SCALE_COUNT", "40000"))
ROUTE_BASE = (23, 23)  # 23.23.x.y/32

def generate_static_routes(duthost, nh):
    routes = {
        "STATIC_ROUTE": {}
    }
    for i in range(ROUTE_COUNT):
        routes["STATIC_ROUTE"][f"default|{generate_ip_address(i)}"] = {
            "blackhole": "false",
            "distance": "0",
            "ifname": "",
            "nexthop": nh,
            "nexthop-vrf": "default"
        }
    return routes

def add_static_routes_to_dut(duthost, routes):
    duthost.copy(content=json.dumps(routes, indent=4), dest="/tmp/static_routes.json")
    duthost.shell("config load -y /tmp/static_routes.json")

def ipv4_nexthop_from_minigraph(duthost, tbinfo):
    mg = duthost.get_extended_minigraph_facts(tbinfo)
    for intf in mg.get("minigraph_interfaces", []):
        peer = intf.get("peer_addr")
        if not peer:
            continue
        peer = peer.split("/")[0]
        try:
            if ipaddress.ip_address(peer).version == 4:
                return peer
        except ValueError:
            continue
    pytest_assert(False, "No IPv4 peer_addr in minigraph_interfaces")


def generate_ip_address(i):
    a, b = ROUTE_BASE
    third, fourth = i // 256, i % 256
    return "{}.{}.{}.{}/32".format(a, b, third, fourth)


def static_route_count_v4(duthost):
    """Parsed `show ip route sum` — see SonicHost.get_ip_route_summary in tests/common/devices/sonic.py."""
    ipv4_summary, _ = duthost.get_ip_route_summary()
    return ipv4_summary.get("static", {}).get("routes", 0)

def remove_static_routes_from_dut(duthost):
    """
    Remove CONFIG_DB STATIC_ROUTE keys in one SSH session.

    A Python loop calling duthost.shell() per key is slow (one round-trip per key).
    Run KEYS | while read; DEL entirely on the DUT instead.
    """
    cmd = (
        'sonic-db-cli CONFIG_DB KEYS "STATIC_ROUTE*" | while read -r k; do '
        '[ -n "$k" ] && sonic-db-cli CONFIG_DB DEL "$k"; '
        "done"
    )
    duthost.shell(cmd)

def test_static_route_scale(rand_selected_dut, tbinfo):
    """
    Add STATIC_ROUTE_SCALE_COUNT static routes; poll until `show ip route sum` static count matches (no loop timeout).
    """
    pytest_assert(ROUTE_COUNT > 0, "STATIC_ROUTE_SCALE_COUNT must be positive")
    pytest_assert(ROUTE_COUNT <= 65536, "Prefix layout supports at most 65536 /32s")

    # init
    duthost = rand_selected_dut
    nh = ipv4_nexthop_from_minigraph(duthost, tbinfo)
    baseline = static_route_count_v4(duthost)
    target = baseline + ROUTE_COUNT

    # add static routes
    logger.info("Adding static routes to DUT")
    time_start = time.time()
    routes = generate_static_routes(duthost, nh)
    add_static_routes_to_dut(duthost, routes)

    # check if static routes are added
    poll_interval = 2
    while True:
        count = static_route_count_v4(duthost)
        logger.info("static route count: %s (want >= %s)", count, target)
        if count >= target:
            break
        time.sleep(poll_interval)

    logger.info(
        "Routes visible in CLI after %.2f s (ROUTE_COUNT=%s)",
        time.time() - time_start,
        ROUTE_COUNT,
    )
    pytest_assert(static_route_count_v4(duthost) >= target, "Static count below target")

    # top snapshot
    logger.info("top snapshot:\n%s", duthost.shell("top -bn1 | head -n 20")["stdout"])

    # Teardown: remove static routes
    logger.info("Removing static routes from DUT")
    time_start = time.time()
    remove_static_routes_from_dut(duthost)
    # check if static routes are removed
    while True:
        count = static_route_count_v4(duthost)
        logger.info("static route count: %s (want 0)", count)
        if count == 0:
            break
        time.sleep(poll_interval)

    logger.info("Static routes removed in %.2f s", time.time() - time_start)
    pytest_assert(static_route_count_v4(duthost) == 0, "Static count not 0 after removal")