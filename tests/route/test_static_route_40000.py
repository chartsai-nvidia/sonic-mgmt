import ipaddress
import logging
import os
import time
import json
import pytest

from tests.common.helpers.assertions import pytest_assert
from tests.common.utilities import wait_until

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.topology("t0", "t1", "any"),
    pytest.mark.device_type("vs"),
]

ROUTE_COUNT = int(os.environ.get("STATIC_ROUTE_SCALE_COUNT", "40000"))
ROUTE_TIMEOUT = int(os.environ.get("STATIC_ROUTE_TIMEOUT", "1800"))
POLL_INTERVAL = 5
BATCH_SIZE = 500

ROUTE_NETWORK = ipaddress.ip_network("23.23.0.0/16")

CPU_MAX_PCT = 20
MEM_MAX_PCT = 20


class TestStaticRouteScale:

    @staticmethod
    def generate_ip_addresses(count):
        """Return *count* /32 prefix strings from ROUTE_NETWORK, skipping .0 and .255 last octets."""
        results = []
        for host in ROUTE_NETWORK.hosts():
            if len(results) >= count:
                break
            results.append("{}/32".format(host))
        return results

    @staticmethod
    def generate_static_routes(nh):
        routes = {
            "STATIC_ROUTE": {}
        }
        ip_addresses = TestStaticRouteScale.generate_ip_addresses(ROUTE_COUNT)
        for ip in ip_addresses:
            routes["STATIC_ROUTE"][f"default|{ip}"] = {
                "blackhole": "false",
                "distance": "0",
                "ifname": "",
                "nexthop": nh,
                "nexthop-vrf": "default"
            }
        return routes

    @staticmethod
    def add_static_routes_to_dut(duthost, nh):
        routes = TestStaticRouteScale.generate_static_routes(nh)
        tmpfile = "/tmp/static_routes.json"
        duthost.copy(content=json.dumps(
            routes, indent=4), dest=tmpfile)
        duthost.shell(f"config load -y {tmpfile}")

    @staticmethod
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

    @staticmethod
    def static_route_count_v4(duthost):
        """Parsed `show ip route sum` — see SonicHost.get_ip_route_summary in tests/common/devices/sonic.py."""
        ipv4_summary, _ = duthost.get_ip_route_summary()
        return ipv4_summary.get("static", {}).get("routes", 0)

    @staticmethod
    def remove_static_routes_from_dut(duthost):
        result = duthost.run_sonic_db_cli_cmd('CONFIG_DB KEYS "STATIC_ROUTE*"')
        keys = [k.strip() for k in result["stdout_lines"] if k.strip()]
        if not keys:
            return
        for i in range(0, len(keys), BATCH_SIZE):
            batch = keys[i:i + BATCH_SIZE]
            del_cmds = "; ".join('sonic-db-cli CONFIG_DB UNLINK "{}"'.format(k)
                                 for k in batch)
            duthost.shell(del_cmds)

    @staticmethod
    def assert_cpu_mem(duthost, cpu_max_pct=CPU_MAX_PCT, mem_max_pct=MEM_MAX_PCT):
        """Assert CPU/memory from a single top snapshot are within thresholds."""
        output = duthost.shell(
            "top -bn1 | awk '"
            "/^%Cpu/{printf \"CPU: %.1f\\n\", 100-$8} "
            "/^[KMG]iB Mem/{printf \"Mem: %.1f\\n\", $8*100/$4}"
            "'",
            module_ignore_errors=True,
        )
        if output["rc"] != 0:
            logger.warning("Failed to run top: %s", output.get("stderr", ""))
            return

        cpu_pct = None
        mem_pct = None
        for line in output["stdout_lines"]:
            if line.startswith("CPU:"):
                try:
                    cpu_pct = float(line.split()[1])
                except (ValueError, IndexError):
                    pass
            elif line.startswith("Mem:"):
                try:
                    mem_pct = float(line.split()[1])
                except (ValueError, IndexError):
                    pass

        logger.info("System usage: CPU=%s%%, Mem=%s%%", cpu_pct, mem_pct)

        failures = []
        if cpu_pct is not None and cpu_pct > cpu_max_pct:
            failures.append(
                "CPU {}% exceeds max {}%".format(cpu_pct, cpu_max_pct))
        if mem_pct is not None and mem_pct > mem_max_pct:
            failures.append(
                "Memory {}% exceeds max {}%".format(mem_pct, mem_max_pct))
        if failures:
            logger.error("Resource usage out of bounds: " +
                         "; ".join(failures))
            pytest.fail("Resource usage out of bounds: " + "; ".join(failures))

    @pytest.fixture
    def static_route_cleanup(self, duthost):
        """Guarantee static routes are removed even if the test fails mid-way."""
        yield
        self.remove_static_routes_from_dut(duthost)

    @staticmethod
    def check_status(duthost, target):
        top_out = duthost.shell(
            "top -bn1 | awk '"
            "/^%Cpu/{printf \"CPU: %.1f%%\", 100-$8} "
            "/^[KMG]iB Mem/{printf \", Mem: %.1f%%\", $8*100/$4}"
            "'")["stdout"]
        logger.info("%s", top_out)
        return TestStaticRouteScale.static_route_count_v4(duthost) == target

    def test_static_route_scale(self, duthost, tbinfo, static_route_cleanup):
        """
        Add ROUTE_COUNT static routes; poll until `show ip route sum` static count matches (no loop timeout).
        """

        nh = self.ipv4_nexthop_from_minigraph(duthost, tbinfo)
        current_static_route_count = self.static_route_count_v4(duthost)
        add_target = current_static_route_count + ROUTE_COUNT
        remove_target = current_static_route_count

        self.assert_cpu_mem(duthost)
        # Add static routes
        logger.info("Adding %d static routes to DUT",
                    ROUTE_COUNT)
        time_start = time.time()
        self.add_static_routes_to_dut(duthost, nh)

        pytest_assert(
            wait_until(ROUTE_TIMEOUT, POLL_INTERVAL, 0,
                       self.check_status, duthost, add_target),
            "Timed out waiting for routes: want >= {}".format(add_target)
        )

        logger.info(
            "Static routes added in %.2f s (target=%s)",
            time.time() - time_start,
            add_target,
        )

        self.assert_cpu_mem(duthost)

        # Remove static routes in batches
        logger.info("Removing static routes from DUT")
        remove_time_start = time.time()
        self.remove_static_routes_from_dut(duthost)

        pytest_assert(
            wait_until(ROUTE_TIMEOUT, POLL_INTERVAL, 0,
                       self.check_status, duthost, remove_target),
            "Timed out waiting for routes to be removed"
        )

        logger.info(
            "Static routes removed in %.2f s (target=%s)",
            time.time() - remove_time_start,
            remove_target,
        )

        self.assert_cpu_mem(duthost)
