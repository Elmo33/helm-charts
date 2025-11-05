from testflows.core import *

import os
import tests.steps.kubernetes as kubernetes
import tests.steps.minikube as minikube
import tests.steps.helm as helm
from tests.steps.deployment import HelmState


@TestScenario
def check_upgrade(self):
    """Test ClickHouse Operator upgrade process."""
    release_name = "upgrade-test"
    namespace = "upgrade-test"
    initial_fixture = "fixtures/upgrade/initial.yaml"
    upgrade_fixture = "fixtures/upgrade/upgrade.yaml"
    
    with Given("paths to fixture files"):
        tests_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        initial_values_path = os.path.join(tests_dir, initial_fixture)
        upgrade_values_path = os.path.join(tests_dir, upgrade_fixture)
    
    with And("define Helm states for initial and upgraded configurations"):
        initial_state = HelmState(initial_values_path)
        upgrade_state = HelmState(upgrade_values_path)

    with When("install ClickHouse with initial configuration"):
        kubernetes.use_context(context_name="minikube")
        helm.install(
            namespace=namespace,
            release_name=release_name,
            values_file=initial_fixture
        )

    with Then("verify initial deployment state"):
        initial_state.verify_all(namespace=namespace)

    with When("upgrade ClickHouse to new configuration"):
        helm.upgrade(
            namespace=namespace,
            release_name=release_name,
            values_file=upgrade_fixture
        )

    with Then("verify upgraded deployment state"):
        upgrade_state.verify_all(namespace=namespace)


@TestFeature
@Name("comprehensive")
def feature(self):
    """Run comprehensive upgrade tests."""

    with Given("minikube environment"):
        minikube.setup_minikube_environment()

    Scenario(run=check_upgrade)
